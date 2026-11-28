#pragma once
#include <mktpack/storage.hpp>
#include "database.hpp"
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace mktpack::storage {

inline constexpr int64_t package_application_id = 0x4d4b5450;

struct TimefileEntry {
    int64_t id = 0;
    int64_t group_id = 0;
    int64_t symbol_id = 0;
    int period = 0;
    int64_t first_time = 0;
    int64_t last_time = 0;
    int64_t rows = 0;
    std::optional<int64_t> volume_id;
    std::string volume_path;
};

bool is_packed(Database& db);
uint64_t sqlite_file_size(Database& db);
void copy_sqlite_file(Database& source, Database& destination);
void replace_file(const std::filesystem::path& from, const std::filesystem::path& to);
int64_t ensure_volume(Database& db, const std::string& path);
void merge_group_schema(Database& db, const std::string& group, const Json& incoming_schema);

std::optional<std::string> meta_get(Database& db, const std::string& key);
void meta_set(Database& db, const std::string& key, const std::string& value);
Json meta_all(Database& db);
uint64_t meta_max_file_size(Database& db, uint64_t fallback = default_max_file_size);
int64_t current_year_start_ms();
std::string today_version();
void ensure_default_version(Database& db);

// 解析 "<prefix><N>" 形式的正整数分卷号（如 "mkt.data." 或 "hot.data."）；不匹配返回 0。
int64_t parse_volume_number(std::string_view path, std::string_view prefix);
bool is_hot_volume(std::string_view path);

// 统一 MKP 载荷读写与删除接口：按目录模式自动路由松散 .mkp 文件、binance 热卷 hot.data.1..M 或直接追加末尾 mkt.data.N。
class PayloadStore {
    Database& db_;
    bool packed_ = false;
    std::string write_prefix_ = "mkt.data.";
    uint64_t max_file_size_ = default_max_file_size;

    // 打包模式下的当前可写热卷状态（独立连接，可在主库事务内自由切卷）。
    int64_t active_hot_num_ = 0;
    int64_t active_hot_vol_id_ = 0;
    std::string active_hot_path_;
    std::unique_ptr<Database> hot_db_;
    std::optional<Statement> hot_insert_;
    bool hot_in_txn_ = false;
    bool hot_batch_mode_ = false;
    size_t hot_entries_ = 0;

    std::map<std::string, std::unique_ptr<Database>> readonly_volumes_;

    Database& readonly_volume(const std::string& volume_path);
    void open_hot_volume(int64_t num, bool create_new);
    void ensure_hot_open();
    void close_hot_volume(bool commit);
    int64_t write_to_hot(int64_t id, std::span<const uint8_t> bytes, const std::string& label);
public:
    explicit PayloadStore(Database& database, uint64_t max_file_size_override = 0);
    PayloadStore(const PayloadStore&) = delete;
    PayloadStore& operator=(const PayloadStore&) = delete;
    ~PayloadStore();

    bool packed() const noexcept { return packed_; }
    uint64_t max_file_size() const noexcept { return max_file_size_; }
    std::filesystem::path loose_path(const std::string& group, const std::string& symbol, int period) const;

    // 批量写入热卷时可开启 batch，减少单条 fsync；必须在主库 COMMIT 前调用 commit_hot_batch()。
    void begin_hot_batch();
    void commit_hot_batch();
    void rollback_hot_batch();

    std::vector<TimefileEntry> list(int64_t group_id, int64_t symbol_id);
    bool exists(int64_t file_id);
    std::vector<uint8_t> read(const TimefileEntry& entry, const std::string& group, const std::string& symbol);

    // 写入或覆盖 MKP 载荷并更新 __timefile__。
    int64_t write_blob(int64_t group_id, const std::string& group,
                       int64_t symbol_id, const std::string& symbol,
                       int period, int64_t first_time, int64_t last_time, int64_t rows,
                       std::span<const uint8_t> bytes);

    // 删除 __timefile__ 记录并清理松散文件或热卷 BLOB；不修改历史冷卷。
    void erase(const TimefileEntry& entry, const std::string& group, const std::string& symbol,
               std::vector<std::filesystem::path>* deferred_unlink = nullptr);

    void close_readonly();
};

}
