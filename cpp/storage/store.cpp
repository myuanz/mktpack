#include "store.hpp"
#include <algorithm>
#include <charconv>
#include <chrono>
#include <stdexcept>

namespace mktpack::storage {
namespace fs = std::filesystem;

bool is_packed(Database& db) {
    Statement check(db, "PRAGMA application_id");
    return check.next() && check.integer(0) == package_application_id;
}

uint64_t sqlite_file_size(Database& db) {
    Statement pages(db, "PRAGMA page_count"), size(db, "PRAGMA page_size");
    pages.next();
    size.next();
    return uint64_t(pages.integer(0)) * uint64_t(size.integer(0));
}

void copy_sqlite_file(Database& source, Database& destination) {
    destination.exec("PRAGMA journal_mode=MEMORY");
    auto* backup = sqlite3_backup_init(destination.db, "main", source.db, "main");
    if (!backup) throw std::runtime_error(sqlite3_errmsg(destination.db));
    const auto status = sqlite3_backup_step(backup, -1);
    const auto finish = sqlite3_backup_finish(backup);
    if (status != SQLITE_DONE || finish != SQLITE_OK)
        throw std::runtime_error("SQLite 复制失败：" + std::string(sqlite3_errmsg(destination.db)));
    destination.exec("PRAGMA journal_mode=MEMORY");
}

void replace_file(const fs::path& from, const fs::path& to) {
    std::error_code ec;
    fs::rename(from, to, ec);
    if (ec) {
        fs::remove(to, ec);
        fs::rename(from, to);
    }
}

int64_t ensure_volume(Database& db, const std::string& path) {
    Statement find(db, "SELECT id FROM __volumes__ WHERE path IN (?,?)");
    find.text(1, path);
    find.text(2, fs::path(path).make_preferred().string());
    if (find.next()) return find.integer(0);
    Statement insert(db, "INSERT INTO __volumes__(path) VALUES(?) RETURNING id");
    insert.text(1, path);
    insert.next();
    return insert.integer(0);
}

void merge_group_schema(Database& db, const std::string& group, const Json& incoming_schema) {
    Json merged = Json::object();
    {
        Statement previous(db, "SELECT schema FROM __group__ WHERE name=?");
        previous.text(1, group);
        if (previous.next()) {
            const auto text = previous.text(0);
            if (!text.empty()) merged = Json::parse(text);
        }
    }
    for (const auto& [name, spec] : incoming_schema.items()) {
        auto& target = merged[name];
        if (target.is_null()) {
            target = spec;
        } else {
            const int old_scale = target.value("scale", 0);
            const int new_scale = spec.value("scale", 0);
            target["scale"] = std::max(old_scale, new_scale);
            if (spec.contains("type")) target["type"] = spec.at("type");
        }
    }
    Statement upsert(db, "INSERT INTO __group__(name,kind,schema) VALUES(?,'timeseries',?) "
                         "ON CONFLICT(name) DO UPDATE SET schema=excluded.schema");
    upsert.text(1, group);
    upsert.text(2, merged.dump());
    upsert.next();
}

namespace {
bool has_meta_table(Database& db) {
    Statement check(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='__meta__'");
    return check.next();
}
}

std::optional<std::string> meta_get(Database& db, const std::string& key) {
    if (!has_meta_table(db)) return std::nullopt;
    Statement q(db, "SELECT value FROM __meta__ WHERE key=?");
    q.text(1, key);
    if (!q.next()) return std::nullopt;
    return q.text(0);
}

void meta_set(Database& db, const std::string& key, const std::string& value) {
    db.exec("CREATE TABLE IF NOT EXISTS __meta__(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID");
    Statement upsert(db, "INSERT INTO __meta__(key,value) VALUES(?,?) "
                         "ON CONFLICT(key) DO UPDATE SET value=excluded.value");
    upsert.text(1, key);
    upsert.text(2, value);
    upsert.next();
}

Json meta_all(Database& db) {
    Json result = Json::object();
    if (!has_meta_table(db)) return result;
    Statement q(db, "SELECT key,value FROM __meta__ ORDER BY key");
    while (q.next()) result[q.text(0)] = q.text(1);
    return result;
}

uint64_t meta_max_file_size(Database& db, uint64_t fallback) {
    const auto text = meta_get(db, "max_file_size");
    if (!text || text->empty()) return fallback;
    uint64_t value = 0;
    const auto [ptr, ec] = std::from_chars(text->data(), text->data() + text->size(), value);
    if (ec != std::errc{} || ptr != text->data() + text->size() || !value) return fallback;
    return value;
}

int64_t current_year_start_ms() {
    using namespace std::chrono;
    const auto today = year_month_day{floor<days>(system_clock::now())};
    return duration_cast<milliseconds>(sys_days{today.year() / January / 1}.time_since_epoch()).count();
}

std::string today_version() {
    using namespace std::chrono;
    const auto today = year_month_day{floor<days>(system_clock::now() + hours(8))};
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04d.%02u.%02u",
                  int(today.year()), unsigned(today.month()), unsigned(today.day()));
    return buf;
}

void ensure_default_version(Database& db) {
    if (!meta_get(db, "version")) meta_set(db, "version", today_version());
}

int64_t parse_volume_number(std::string_view path, std::string_view prefix) {
    if (!path.starts_with(prefix)) return 0;
    const auto rest = path.substr(prefix.size());
    if (rest.empty()) return 0;
    int64_t value = 0;
    const auto [ptr, ec] = std::from_chars(rest.data(), rest.data() + rest.size(), value);
    if (ec != std::errc{} || ptr != rest.data() + rest.size() || value <= 0) return 0;
    return value;
}

bool is_hot_volume(std::string_view path) {
    return path == "hot.data" || parse_volume_number(path, "hot.data.") > 0;
}

PayloadStore::PayloadStore(Database& database, uint64_t max_file_size_override)
    : db_(database), packed_(is_packed(database)) {
    max_file_size_ = max_file_size_override ? max_file_size_override : meta_max_file_size(db_);
    if (meta_get(db_, "port").value_or("") == "binance") write_prefix_ = "hot.data.";
}

PayloadStore::~PayloadStore() {
    close_readonly();
    try {
        close_hot_volume(false);
    } catch (...) {}
}

void PayloadStore::open_hot_volume(int64_t num, bool create_new) {
    close_hot_volume(true);
    active_hot_num_ = num;
    active_hot_path_ = write_prefix_ + std::to_string(num);
    active_hot_vol_id_ = ensure_volume(db_, active_hot_path_);
    readonly_volumes_.erase(active_hot_path_);

    hot_db_ = std::make_unique<Database>(db_.root / active_hot_path_,
                                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
    hot_db_->exec("PRAGMA journal_mode=MEMORY;CREATE TABLE IF NOT EXISTS blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
    if (create_new) {
        hot_entries_ = 0;
    } else {
        Statement cnt(*hot_db_, "SELECT count(*) FROM blob");
        hot_entries_ = cnt.next() ? static_cast<size_t>(cnt.integer(0)) : 0;
    }
    hot_insert_.emplace(*hot_db_, "INSERT OR REPLACE INTO blob(id,bytes) VALUES(?,?)");
    if (hot_batch_mode_) {
        hot_db_->exec("BEGIN IMMEDIATE");
        hot_in_txn_ = true;
    }
}

void PayloadStore::ensure_hot_open() {
    if (hot_db_) return;
    int64_t max_num = 0;
    Statement q(db_, "SELECT path FROM __volumes__");
    while (q.next()) {
        max_num = std::max(max_num, parse_volume_number(q.text(0), write_prefix_));
    }
    if (max_num == 0) open_hot_volume(1, true);
    else open_hot_volume(max_num, false);
}

void PayloadStore::close_hot_volume(bool commit) {
    if (!hot_db_) return;
    hot_insert_.reset();
    if (hot_in_txn_) {
        if (commit) hot_db_->exec("COMMIT");
        else sqlite3_exec(hot_db_->db, "ROLLBACK", nullptr, nullptr, nullptr);
        hot_in_txn_ = false;
    }
    hot_db_.reset();
}

void PayloadStore::begin_hot_batch() {
    if (!packed_) return;
    hot_batch_mode_ = true;
    if (hot_db_ && !hot_in_txn_) {
        hot_db_->exec("BEGIN IMMEDIATE");
        hot_in_txn_ = true;
    }
}

void PayloadStore::commit_hot_batch() {
    if (!packed_) return;
    if (hot_db_ && hot_in_txn_) {
        hot_db_->exec("COMMIT");
        hot_in_txn_ = false;
    }
    hot_batch_mode_ = false;
}

void PayloadStore::rollback_hot_batch() {
    if (!packed_) return;
    if (hot_db_ && hot_in_txn_) {
        sqlite3_exec(hot_db_->db, "ROLLBACK", nullptr, nullptr, nullptr);
        hot_in_txn_ = false;
    }
    hot_batch_mode_ = false;
}

fs::path PayloadStore::loose_path(const std::string& group, const std::string& symbol, int period) const {
    return db_.root / group / symbol / (std::to_string(period) + ".mkp");
}

Database& PayloadStore::readonly_volume(const std::string& volume_path) {
    if (hot_db_ && volume_path == active_hot_path_) return *hot_db_;
    auto& vol = readonly_volumes_[volume_path];
    if (!vol) {
        vol = std::make_unique<Database>(db_.root / fs::path(volume_path),
                                         SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX);
    }
    return *vol;
}

void PayloadStore::close_readonly() {
    readonly_volumes_.clear();
}

std::vector<TimefileEntry> PayloadStore::list(int64_t group_id, int64_t symbol_id) {
    Statement q(db_, "SELECT f.id,f.group_id,f.symbol_id,f.period,f.first_time,f.last_time,f.rows,"
                     "f.volume_id,coalesce(v.path,'') "
                     "FROM __timefile__ f LEFT JOIN __volumes__ v ON v.id=f.volume_id "
                     "WHERE f.group_id=? AND f.symbol_id=? ORDER BY f.period,f.id");
    q.integer(1, group_id);
    q.integer(2, symbol_id);
    std::vector<TimefileEntry> result;
    while (q.next()) {
        std::optional<int64_t> vol;
        if (sqlite3_column_type(q.statement, 7) != SQLITE_NULL) vol = q.integer(7);
        result.push_back({q.integer(0), q.integer(1), q.integer(2), int(q.integer(3)),
                          q.integer(4), q.integer(5), q.integer(6), vol, q.text(8)});
    }
    return result;
}

bool PayloadStore::exists(int64_t file_id) {
    Statement q(db_, "SELECT g.name,s.name,f.period,f.volume_id,coalesce(v.path,'') "
                     "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id "
                     "JOIN __symbols__ s ON s.id=f.symbol_id "
                     "LEFT JOIN __volumes__ v ON v.id=f.volume_id WHERE f.id=?");
    q.integer(1, file_id);
    if (!q.next()) return false;
    if (sqlite3_column_type(q.statement, 3) == SQLITE_NULL) {
        return fs::exists(loose_path(q.text(0), q.text(1), int(q.integer(2))));
    }
    const auto volume_path = q.text(4);
    if (volume_path.empty()) return false;
    const auto full_path = db_.root / fs::path(volume_path);
    if (!fs::exists(full_path)) return false;
    Statement check(readonly_volume(volume_path), "SELECT 1 FROM blob WHERE id=?");
    check.integer(1, file_id);
    return check.next();
}

std::vector<uint8_t> PayloadStore::read(const TimefileEntry& entry,
                                        const std::string& group, const std::string& symbol) {
    if (!entry.volume_id || entry.volume_path.empty()) {
        return read_file_bytes(loose_path(group, symbol, entry.period));
    }
    const auto label = group + "/" + symbol + "/" + std::to_string(entry.period) + ".mkp";
    Statement q(readonly_volume(entry.volume_path), "SELECT bytes FROM blob WHERE id=?");
    return read_blob(q, entry.id, label);
}

int64_t PayloadStore::write_to_hot(int64_t id, std::span<const uint8_t> bytes, const std::string& label) {
    for (;;) {
        ensure_hot_open();
        const bool local_txn = !hot_in_txn_;
        if (local_txn) {
            hot_db_->exec("BEGIN IMMEDIATE");
            hot_in_txn_ = true;
        }
        hot_db_->exec("SAVEPOINT entry");
        insert_blob(*hot_insert_, id, bytes);
        const auto size = sqlite_file_size(*hot_db_);
        if (size <= max_file_size_) {
            hot_db_->exec("RELEASE entry");
            ++hot_entries_;
            if (local_txn) {
                const int64_t vol_id = active_hot_vol_id_;
                close_hot_volume(true);
                return vol_id;
            }
            return active_hot_vol_id_;
        }
        hot_db_->exec("ROLLBACK TO entry;RELEASE entry");
        if (!hot_entries_) {
            if (local_txn) {
                hot_db_->exec("ROLLBACK");
                hot_in_txn_ = false;
            }
            throw std::runtime_error(label + "需要至少 " + std::to_string(size) +
                                     " 字节热卷，超过分卷上限 " + std::to_string(max_file_size_));
        }
        open_hot_volume(active_hot_num_ + 1, true);
    }
}

int64_t PayloadStore::write_blob(int64_t group_id, const std::string& group,
                                 int64_t symbol_id, const std::string& symbol,
                                 int period, int64_t first_time, int64_t last_time, int64_t rows,
                                 std::span<const uint8_t> bytes) {
    if (!packed_) {
        const auto target = loose_path(group, symbol, period);
        fs::create_directories(target.parent_path());
        auto temporary = target;
        temporary += ".tmp";
        {
            std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
            if (!output) throw std::runtime_error("无法写入：" + utf8_string(temporary));
            if (!bytes.empty()) output.write(reinterpret_cast<const char*>(bytes.data()),
                                             static_cast<std::streamsize>(bytes.size()));
            if (!output) throw std::runtime_error("载荷写入失败：" + utf8_string(temporary));
        }
        replace_file(temporary, target);
        Statement upsert(db_, "INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows,volume_id) "
                              "VALUES(?,?,?,?,?,?,NULL) "
                              "ON CONFLICT(group_id,symbol_id,period) DO UPDATE "
                              "SET first_time=excluded.first_time,last_time=excluded.last_time,"
                              "rows=excluded.rows,volume_id=NULL RETURNING id");
        upsert.integer(1, group_id);
        upsert.integer(2, symbol_id);
        upsert.integer(3, period);
        upsert.integer(4, first_time);
        upsert.integer(5, last_time);
        upsert.integer(6, rows);
        upsert.next();
        return upsert.integer(0);
    }

    // 先在 __timefile__ 中获取或分配稳定 id，再按 max_file_size 写入当前热卷并回填 volume_id。
    Statement upsert(db_, "INSERT INTO __timefile__(group_id,symbol_id,period,first_time,last_time,rows,volume_id) "
                          "VALUES(?,?,?,?,?,?,NULL) "
                          "ON CONFLICT(group_id,symbol_id,period) DO UPDATE "
                          "SET first_time=excluded.first_time,last_time=excluded.last_time,"
                          "rows=excluded.rows RETURNING id");
    upsert.integer(1, group_id);
    upsert.integer(2, symbol_id);
    upsert.integer(3, period);
    upsert.integer(4, first_time);
    upsert.integer(5, last_time);
    upsert.integer(6, rows);
    upsert.next();
    const int64_t id = upsert.integer(0);

    const auto label = group + "/" + symbol + "/" + std::to_string(period) + ".mkp";
    const int64_t vol_id = write_to_hot(id, bytes, label);

    Statement set_vol(db_, "UPDATE __timefile__ SET volume_id=? WHERE id=?");
    set_vol.integer(1, vol_id);
    set_vol.integer(2, id);
    set_vol.next();
    return id;
}

void PayloadStore::erase(const TimefileEntry& entry, const std::string& group, const std::string& symbol,
                         std::vector<fs::path>* deferred_unlink) {
    Statement remove(db_, "DELETE FROM __timefile__ WHERE id=?");
    remove.integer(1, entry.id);
    remove.next();
    if (!entry.volume_id || entry.volume_path.empty()) {
        const auto path = loose_path(group, symbol, entry.period);
        if (deferred_unlink) deferred_unlink->push_back(path);
        else {
            std::error_code ec;
            fs::remove(path, ec);
        }
        return;
    }
    if (hot_db_ && entry.volume_path == active_hot_path_) {
        Statement del(*hot_db_, "DELETE FROM blob WHERE id=?");
        del.integer(1, entry.id);
        del.next();
    }
}

}
