#include <mktpack/storage.hpp>
#include "sqlite.hpp"
#include "store.hpp"
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace mktpack {
namespace {
using namespace storage;
namespace fs = std::filesystem;

void copy_inline_ordinary(Database& src, Database& dst, const GroupSpec& spec) {
    Statement ddl(src, "SELECT sql FROM sqlite_master WHERE type='table' AND name=?");
    ddl.text(1, spec.table);
    if (!ddl.next()) throw std::runtime_error("源普通表不存在：" + spec.table);
    const auto create_sql = ddl.text(0);

    Statement query(src, "SELECT * FROM " + identifier(spec.table));
    const int ncols = sqlite3_column_count(query.statement);
    std::vector<std::string> col_names;
    col_names.reserve(ncols);
    for (int i = 0; i < ncols; ++i) col_names.emplace_back(sqlite3_column_name(query.statement, i));
    std::vector<std::vector<Json>> rows;
    while (query.next()) {
        std::vector<Json> row;
        row.reserve(ncols);
        for (int i = 0; i < ncols; ++i) row.push_back(sql_value(query.statement, i));
        rows.push_back(std::move(row));
    }

    dst.exec("DROP TABLE IF EXISTS " + identifier(spec.table));
    dst.exec(create_sql);
    if (!rows.empty()) {
        std::string cols, params;
        for (size_t i = 0; i < col_names.size(); ++i) {
            if (i) { cols += ","; params += ","; }
            cols += identifier(col_names[i]);
            params += "?";
        }
        Statement insert(dst, "INSERT INTO " + identifier(spec.table) + "(" + cols + ") VALUES(" + params + ")");
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); ++i) bind_value(insert, int(i + 1), row[i]);
            insert.next();
            sqlite3_reset(insert.statement);
        }
    }
    Statement upsert(dst, "INSERT INTO __group__(name,kind,storage,location,table_name,schema) "
                          "VALUES(?,'ordinary','sqlite','',?,?) "
                          "ON CONFLICT(name) DO UPDATE SET kind=excluded.kind,storage=excluded.storage,"
                          "location=excluded.location,table_name=excluded.table_name,schema=excluded.schema");
    upsert.text(1, spec.name);
    upsert.text(2, spec.table == spec.name ? "" : spec.table);
    upsert.text(3, spec.schema.dump());
    upsert.next();
}

// 通用分卷写入器：支持直接向未满末尾冷卷追加，或按临时后缀写出后原子替换热卷。
class VolumeAppender {
    fs::path root_;
    std::string prefix_;
    std::string temp_suffix_;
    uint64_t limit_;
    int64_t next_id_;
    std::unique_ptr<Database> current_;
    std::optional<Statement> insert_;
    std::string current_name_;
    bool appending_existing_ = false;
    size_t entries_ = 0;
public:
    std::vector<std::pair<fs::path, std::string>> created; // (disk_path, final_name)
    bool appended_tail = false;

    VolumeAppender(fs::path root, std::string prefix, uint64_t limit, int64_t start_id,
                   std::string temp_suffix = "",
                   std::optional<std::string> existing_tail = std::nullopt)
        : root_(std::move(root)), prefix_(std::move(prefix)), temp_suffix_(std::move(temp_suffix)),
          limit_(limit), next_id_(start_id) {
        if (existing_tail && !existing_tail->empty()) {
            auto vol = std::make_unique<Database>(root_ / fs::path(*existing_tail), SQLITE_OPEN_READWRITE);
            if (sqlite_file_size(*vol) < limit_) {
                vol->exec("PRAGMA journal_mode=MEMORY;BEGIN IMMEDIATE");
                Statement cnt(*vol, "SELECT count(*) FROM blob");
                entries_ = cnt.next() ? static_cast<size_t>(cnt.integer(0)) : 0;
                insert_.emplace(*vol, "INSERT OR REPLACE INTO blob(id,bytes) VALUES(?,?)");
                current_ = std::move(vol);
                current_name_ = std::move(*existing_tail);
                appending_existing_ = true;
            }
        }
    }

    void close(bool commit = true) {
        if (!current_) return;
        insert_.reset();
        if (commit) current_->exec("COMMIT");
        else sqlite3_exec(current_->db, "ROLLBACK", nullptr, nullptr, nullptr);
        current_.reset();
        current_name_.clear();
        appending_existing_ = false;
        entries_ = 0;
    }

    std::string append(int64_t id, std::span<const uint8_t> payload, const std::string& label) {
        for (;;) {
            if (!current_) {
                current_name_ = prefix_ + std::to_string(next_id_++);
                const auto disk_path = root_ / (current_name_ + temp_suffix_);
                if (!temp_suffix_.empty()) {
                    std::error_code ec;
                    fs::remove(disk_path, ec);
                }
                current_ = std::make_unique<Database>(disk_path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
                current_->exec("PRAGMA journal_mode=MEMORY;BEGIN;CREATE TABLE blob(id INTEGER PRIMARY KEY,bytes BLOB NOT NULL)");
                insert_.emplace(*current_, "INSERT INTO blob(id,bytes) VALUES(?,?)");
                created.emplace_back(disk_path, current_name_);
                appending_existing_ = false;
                entries_ = 0;
            }
            current_->exec("SAVEPOINT entry");
            insert_blob(*insert_, id, payload);
            const auto size = sqlite_file_size(*current_);
            if (size <= limit_) {
                current_->exec("RELEASE entry");
                ++entries_;
                if (appending_existing_) appended_tail = true;
                return current_name_;
            }
            current_->exec("ROLLBACK TO entry;RELEASE entry");
            if (!entries_)
                throw std::runtime_error(label + "需要至少 " + std::to_string(size) +
                                         " 字节分卷，超过分卷上限 " + std::to_string(limit_));
            close(true);
        }
    }
};
}

Json dataset_meta(const fs::path& root) {
    Database db(root);
    return meta_all(db);
}

void set_dataset_meta(const fs::path& root, const std::string& key, const std::string& value) {
    Database db(root, SQLITE_OPEN_READWRITE);
    db.exec("BEGIN IMMEDIATE");
    try {
        meta_set(db, key, value);
        db.exec("COMMIT");
    } catch (...) {
        sqlite3_exec(db.db, "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }
}

Json merge_dataset(const fs::path& target, const fs::path& source, const MergeOptions& options) {
    const auto target_index = fs::is_directory(target) ? target / "mkt.sqlite" : target;
    if (!fs::exists(target_index))
        throw std::runtime_error("目标数据集不存在：" + target_index.string());

    Database src(source);
    Database dst(target, SQLITE_OPEN_READWRITE);
    create_index(dst);
    PayloadStore store(dst);

    std::set<std::string> requested_groups(options.groups.begin(), options.groups.end());
    std::set<std::string> requested_symbols(options.symbols.begin(), options.symbols.end());

    std::set<std::string> known_groups, mkp_groups;
    std::vector<GroupSpec> ordinary_groups;
    Json warnings = Json::array();

    {
        Statement groups(src, "SELECT name,kind,storage FROM __group__ ORDER BY name");
        while (groups.next()) {
            const auto name = groups.text(0);
            const auto kind = groups.text(1);
            const auto storage_kind = groups.text(2);
            known_groups.insert(name);
            const bool selected = requested_groups.empty() || requested_groups.contains(name);
            if (!selected) continue;
            if (kind == "ordinary") {
                ordinary_groups.push_back(group_spec(src, name));
            } else if (kind == "timeseries" && storage_kind == "mkp") {
                mkp_groups.insert(name);
            } else if (kind == "timeseries" && storage_kind == "sqlite") {
                // TODO: 支持 SQLite 时序数据的增量合并（symbol_id 重映射与分区更新）
                if (!requested_groups.empty())
                    throw std::runtime_error("暂不支持合并 SQLite 时序组：" + name);
                warnings.push_back("暂不支持合并 SQLite 时序组：" + name);
            }
        }
        for (const auto& name : requested_groups)
            if (!known_groups.contains(name)) throw std::runtime_error("源数据集中未知数据组：" + name);
    }

    size_t merged_files = 0, inserted_files = 0, updated_files = 0, merged_ordinary = 0;
    int64_t merged_rows = 0;

    std::map<std::string, std::unique_ptr<Database>> src_volumes;
    auto read_src_payload = [&](int64_t id, const std::string& group, const std::string& symbol,
                                int period, const std::string& volume_path) -> std::vector<uint8_t> {
        if (volume_path.empty()) {
            return read_file_bytes(src.root / group / symbol / (std::to_string(period) + ".mkp"));
        }
        auto& vol = src_volumes[volume_path];
        if (!vol) {
            vol = std::make_unique<Database>(src.root / fs::path(volume_path),
                                             SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX);
        }
        Statement q(*vol, "SELECT bytes FROM blob WHERE id=?");
        return read_blob(q, id, group + "/" + symbol + "/" + std::to_string(period) + ".mkp");
    };

    std::vector<GroupSpec> inline_ordinary;
    for (const auto& spec : ordinary_groups) {
        if (!spec.location.empty()) {
            const auto dest_path = dst.root / spec.location;
            fs::create_directories(dest_path.parent_path());
            Database s(src.root / spec.location);
            Database d(dest_path, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
            copy_sqlite_file(s, d);
            ++merged_ordinary;
        } else {
            inline_ordinary.push_back(spec);
        }
    }

    std::map<std::string, int64_t> dst_groups, dst_symbols;
    store.begin_hot_batch();
    dst.exec("BEGIN IMMEDIATE");
    try {
        for (const auto& spec : ordinary_groups) {
            if (!spec.location.empty()) {
                Statement upsert(dst, "INSERT INTO __group__(name,kind,storage,location,table_name,schema) "
                                      "VALUES(?,'ordinary','sqlite',?,?,?) "
                                      "ON CONFLICT(name) DO UPDATE SET kind=excluded.kind,storage=excluded.storage,"
                                      "location=excluded.location,table_name=excluded.table_name,schema=excluded.schema");
                upsert.text(1, spec.name);
                upsert.text(2, spec.location);
                upsert.text(3, spec.table == spec.name ? "" : spec.table);
                upsert.text(4, spec.schema.dump());
                upsert.next();
            }
        }
        for (const auto& spec : inline_ordinary) {
            copy_inline_ordinary(src, dst, spec);
            ++merged_ordinary;
        }
        for (const auto& name : mkp_groups) {
            const auto spec = group_spec(src, name);
            merge_group_schema(dst, name, spec.schema);
            dst_groups[name] = storage::group_id(dst, name);
        }

        if (!mkp_groups.empty()) {
            Statement exists_stmt(dst, "SELECT 1 FROM __timefile__ WHERE group_id=? AND symbol_id=? AND period=?");
            Statement files(src, "SELECT f.id,g.name,s.name,f.period,f.first_time,f.last_time,f.rows,coalesce(v.path,'') "
                                 "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id "
                                 "JOIN __symbols__ s ON s.id=f.symbol_id "
                                 "LEFT JOIN __volumes__ v ON v.id=f.volume_id "
                                 "ORDER BY v.path,f.id");
            while (files.next()) {
                const auto group = files.text(1);
                if (!mkp_groups.contains(group)) continue;
                const auto symbol = files.text(2);
                if (!requested_symbols.empty() && !requested_symbols.contains(symbol)) continue;
                const int64_t src_id = files.integer(0);
                const int period = int(files.integer(3));
                const int64_t first_time = files.integer(4);
                const int64_t last_time = files.integer(5);
                const int64_t rows = files.integer(6);
                const auto vol_path = files.text(7);

                auto [sym_it, is_new_sym] = dst_symbols.try_emplace(symbol, 0);
                if (is_new_sym) sym_it->second = storage::symbol_id(dst, symbol);
                const int64_t gid = dst_groups.at(group);
                const int64_t sid = sym_it->second;

                exists_stmt.integer(1, gid);
                exists_stmt.integer(2, sid);
                exists_stmt.integer(3, period);
                const bool existed = exists_stmt.next();
                sqlite3_reset(exists_stmt.statement);

                const auto bytes = read_src_payload(src_id, group, symbol, period, vol_path);
                store.write_blob(gid, group, sid, symbol, period, first_time, last_time, rows, bytes);
                ++merged_files;
                if (existed) ++updated_files;
                else ++inserted_files;
                merged_rows += rows;
                if (options.progress)
                    options.progress("合并 " + group + "/" + symbol + "/" + std::to_string(period));
            }
        }
        store.commit_hot_batch();
        dst.exec("COMMIT");
    } catch (...) {
        store.rollback_hot_batch();
        sqlite3_exec(dst.db, "ROLLBACK", nullptr, nullptr, nullptr);
        throw;
    }

    return {
        {"files", merged_files},
        {"inserted", inserted_files},
        {"updated", updated_files},
        {"rows", merged_rows},
        {"ordinary", merged_ordinary},
        {"packed", store.packed()},
        {"warnings", warnings}
    };
}

Json compact(const fs::path& root, const CompactOptions& options) {
    Database db(root, SQLITE_OPEN_READWRITE);
    if (!is_packed(db)) throw std::runtime_error("compact 仅适用于已打包数据集");

    const uint64_t limit = options.max_file_size ? options.max_file_size : meta_max_file_size(db);
    if (!limit) throw std::runtime_error("分卷上限必须大于零");

    const auto port = meta_get(db, "port").value_or("");
    const int64_t before = options.before ? *options.before
        : (port == "binance" ? current_year_start_ms() : std::numeric_limits<int64_t>::max());

    std::map<int64_t, std::string> hot_volumes;
    std::set<int64_t> sql_volumes;
    {
        Statement q(db, "SELECT DISTINCT volume_id FROM __sqlite_partitions__ WHERE volume_id IS NOT NULL");
        while (q.next()) sql_volumes.insert(q.integer(0));
    }

    int64_t max_data_num = 0;
    std::optional<std::string> tail_cold_path;
    {
        Statement q(db, "SELECT id,path FROM __volumes__");
        while (q.next()) {
            const int64_t id = q.integer(0);
            const auto path = q.text(1);
            if (is_hot_volume(path)) {
                hot_volumes.emplace(id, path);
            } else {
                const int64_t num = parse_volume_number(path, "mkt.data.");
                if (num > max_data_num) {
                    max_data_num = num;
                    if (!sql_volumes.contains(id)) tail_cold_path = path;
                    else tail_cold_path.reset();
                }
            }
        }
    }

    struct Item {
        int64_t id, first_time, last_time, rows;
        int period;
        std::optional<int64_t> volume_id;
        std::string volume_path, group_name, symbol_name;
    };

    std::vector<Item> candidates;
    std::vector<fs::path> loose_to_remove;
    {
        Statement q(db, "SELECT f.id,f.period,f.first_time,f.last_time,f.rows,"
                        "f.volume_id,coalesce(v.path,''),g.name,s.name "
                        "FROM __timefile__ f JOIN __group__ g ON g.id=f.group_id "
                        "JOIN __symbols__ s ON s.id=f.symbol_id "
                        "LEFT JOIN __volumes__ v ON v.id=f.volume_id");
        while (q.next()) {
            std::optional<int64_t> vid;
            if (sqlite3_column_type(q.statement, 5) != SQLITE_NULL) vid = q.integer(5);
            const auto vpath = q.text(6);
            if (vid && !is_hot_volume(vpath)) continue;
            Item item{q.integer(0), q.integer(2), q.integer(3), q.integer(4),
                      int(q.integer(1)), vid, vpath, q.text(7), q.text(8)};
            if (!vid) {
                loose_to_remove.push_back(db.root / item.group_name / item.symbol_name /
                                          (std::to_string(item.period) + ".mkp"));
            }
            candidates.push_back(std::move(item));
        }
    }

    std::vector<const Item*> cold_items, hot_items;
    for (const auto& item : candidates) {
        if (item.last_time < before) cold_items.push_back(&item);
        else hot_items.push_back(&item);
    }

    const auto by_logical_order = [](const Item* a, const Item* b) {
        return std::tie(a->group_name, a->symbol_name, a->period, a->id) <
               std::tie(b->group_name, b->symbol_name, b->period, b->id);
    };
    std::sort(cold_items.begin(), cold_items.end(), by_logical_order);
    std::sort(hot_items.begin(), hot_items.end(), by_logical_order);

    if (cold_items.empty() && loose_to_remove.empty() && hot_volumes.empty()) {
        return {
            {"cold_files", 0},
            {"hot_files", 0},
            {"created_volumes", 0},
            {"hot_volumes", 0},
            {"appended_tail", false}
        };
    }

    std::map<std::string, std::unique_ptr<Database>> source_volumes;
    auto read_item = [&](const Item& item) -> std::vector<uint8_t> {
        if (!item.volume_id || item.volume_path.empty()) {
            return read_file_bytes(db.root / item.group_name / item.symbol_name /
                                   (std::to_string(item.period) + ".mkp"));
        }
        auto& vol = source_volumes[item.volume_path];
        if (!vol) {
            vol = std::make_unique<Database>(db.root / fs::path(item.volume_path),
                                             SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX);
        }
        Statement q(*vol, "SELECT bytes FROM blob WHERE id=?");
        return read_blob(q, item.id, item.group_name + "/" + item.symbol_name + "/" + std::to_string(item.period) + ".mkp");
    };

    VolumeAppender cold_writer(db.root, "mkt.data.", limit, max_data_num + 1, "",
                               cold_items.empty() ? std::nullopt : tail_cold_path);
    VolumeAppender hot_writer(db.root, "hot.data.", limit, 1, ".compact.tmp");
    std::vector<std::pair<int64_t, std::string>> cold_assignments, hot_assignments;
    cold_assignments.reserve(cold_items.size());
    hot_assignments.reserve(hot_items.size());

    try {
        for (size_t i = 0; i < cold_items.size(); ++i) {
            const auto& item = *cold_items[i];
            const auto label = item.group_name + "/" + item.symbol_name + "/" + std::to_string(item.period) + ".mkp";
            const auto bytes = read_item(item);
            const auto vpath = cold_writer.append(item.id, bytes, label);
            cold_assignments.emplace_back(item.id, vpath);
            if (options.progress)
                options.progress("归档 " + label + " → " + vpath +
                                 " (" + std::to_string(i + 1) + "/" + std::to_string(cold_items.size()) + ")");
        }
        cold_writer.close(true);

        for (const auto* item : hot_items) {
            const auto label = item->group_name + "/" + item->symbol_name + "/" + std::to_string(item->period) + ".mkp";
            const auto bytes = read_item(*item);
            const auto vpath = hot_writer.append(item->id, bytes, label);
            hot_assignments.emplace_back(item->id, vpath);
        }
        hot_writer.close(true);

        // 先释放所有源热卷只读句柄，确保 Windows 可替换/删除旧热卷文件。
        source_volumes.clear();

        std::set<std::string> new_hot_names;
        for (const auto& [tmp, name] : hot_writer.created) new_hot_names.insert(name);

        db.exec("BEGIN IMMEDIATE");
        try {
            std::map<std::string, int64_t> vol_ids;
            for (const auto& [id, vpath] : cold_assignments) {
                if (!vol_ids.contains(vpath)) vol_ids[vpath] = ensure_volume(db, vpath);
            }
            for (const auto& [id, vpath] : hot_assignments) {
                if (!vol_ids.contains(vpath)) vol_ids[vpath] = ensure_volume(db, vpath);
            }
            Statement update(db, "UPDATE __timefile__ SET volume_id=? WHERE id=?");
            for (const auto& [id, vpath] : cold_assignments) {
                update.integer(1, vol_ids.at(vpath));
                update.integer(2, id);
                update.next();
                sqlite3_reset(update.statement);
            }
            for (const auto& [id, vpath] : hot_assignments) {
                update.integer(1, vol_ids.at(vpath));
                update.integer(2, id);
                update.next();
                sqlite3_reset(update.statement);
            }
            Statement del_vol(db, "DELETE FROM __volumes__ WHERE id=?");
            for (const auto& [vid, vpath] : hot_volumes) {
                if (new_hot_names.contains(vpath)) continue;
                del_vol.integer(1, vid);
                del_vol.next();
                sqlite3_reset(del_vol.statement);
            }
            db.exec("COMMIT");
        } catch (...) {
            sqlite3_exec(db.db, "ROLLBACK", nullptr, nullptr, nullptr);
            throw;
        }
    } catch (...) {
        cold_writer.close(false);
        hot_writer.close(false);
        source_volumes.clear();
        std::error_code ec;
        for (const auto& [disk_path, name] : hot_writer.created) fs::remove(disk_path, ec);
        for (const auto& [disk_path, name] : cold_writer.created) fs::remove(disk_path, ec);
        throw;
    }

    // 事务已提交，原子替换新热卷并清理已弃用的旧热卷与松散文件。
    std::error_code ec;
    std::set<std::string> new_hot_names;
    for (const auto& [tmp, name] : hot_writer.created) {
        new_hot_names.insert(name);
        replace_file(tmp, db.root / name);
    }
    for (const auto& [vid, vpath] : hot_volumes) {
        if (!new_hot_names.contains(vpath)) fs::remove(db.root / fs::path(vpath), ec);
    }
    for (const auto& path : loose_to_remove) {
        fs::remove(path, ec);
    }

    return {
        {"cold_files", cold_items.size()},
        {"hot_files", hot_items.size()},
        {"created_volumes", cold_writer.created.size()},
        {"hot_volumes", hot_writer.created.size()},
        {"appended_tail", cold_writer.appended_tail}
    };
}

}
