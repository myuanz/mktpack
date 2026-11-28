#pragma once
#include <mktpack/core.hpp>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
namespace mktpack {
// 支持无零初始化预分配的字节缓冲区，避免大结果集在单线程触发全量清零与软缺页。
class EmptyBuffer {
    std::unique_ptr<uint8_t[]> data_;
    size_t size_ = 0;
    size_t capacity_ = 0;

    void grow(size_t needed) {
        size_t next = capacity_ ? capacity_ * 2 : 64;
        if (next < needed) next = needed;
        auto fresh = std::make_unique_for_overwrite<uint8_t[]>(next);
        if (size_) std::memcpy(fresh.get(), data_.get(), size_);
        data_ = std::move(fresh);
        capacity_ = next;
    }
public:
    EmptyBuffer() = default;
    EmptyBuffer(EmptyBuffer&& other) noexcept
        : data_(std::move(other.data_)), size_(other.size_), capacity_(other.capacity_) {
        other.size_ = other.capacity_ = 0;
    }
    EmptyBuffer& operator=(EmptyBuffer&& other) noexcept {
        if (this != &other) {
            data_ = std::move(other.data_);
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.size_ = other.capacity_ = 0;
        }
        return *this;
    }
    EmptyBuffer(const EmptyBuffer&) = delete;
    EmptyBuffer& operator=(const EmptyBuffer&) = delete;

    uint8_t* data() noexcept { return data_.get(); }
    const uint8_t* data() const noexcept { return data_.get(); }
    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }
    uint8_t& operator[](size_t index) noexcept { return data_[index]; }
    const uint8_t& operator[](size_t index) const noexcept { return data_[index]; }

    void reserve(size_t capacity) {
        if (capacity > capacity_) grow(capacity);
    }
    void resize_for_overwrite(size_t size) {
        if (size > capacity_) grow(size);
        size_ = size;
    }
    void resize(size_t size, uint8_t fill = 0) {
        const size_t old = size_;
        resize_for_overwrite(size);
        if (size > old) std::memset(data_.get() + old, fill, size - old);
    }
    void push_back(uint8_t byte) {
        if (size_ == capacity_) grow(size_ + 1);
        data_[size_++] = byte;
    }
    void append(const void* src, size_t length) {
        if (!length) return;
        const size_t offset = size_;
        if (offset + length > capacity_) grow(offset + length);
        std::memcpy(data_.get() + offset, src, length);
        size_ = offset + length;
    }
};

Json describe(const std::filesystem::path& root,const std::string& group="",const std::string& symbol="");
std::vector<std::string> groups(const std::filesystem::path& root);
std::optional<std::string> group_kind(const std::filesystem::path& root,const std::string& group);
std::vector<std::string> symbols(const std::filesystem::path& root,const std::string& group,
                                 int64_t start=std::numeric_limits<int64_t>::min(),
                                 int64_t end=std::numeric_limits<int64_t>::max());
Json ordinary(const std::filesystem::path& root,const std::string& group);
// Arrow 数组的列类型、有效性位图、数据缓冲区及嵌套子列。
struct ResultColumn {
    std::string type;
    int scale = 0;
    size_t rows = 0;
    std::vector<EmptyBuffer> buffers;
    std::vector<ResultColumn> children;
};
struct QueryResult {
    size_t rows = 0;
    std::vector<std::string> names;
    std::vector<ResultColumn> columns;
};
using Projection = std::optional<std::vector<std::string>>;
QueryResult query(const std::filesystem::path& root, const std::string& group,
                  const std::vector<std::string>& symbols, int64_t start, int64_t end,
                  const Projection& requested = {});
QueryResult file_result(const std::filesystem::path& path);
Json check_dataset(const std::filesystem::path& root);
inline constexpr uint64_t default_max_file_size=8ULL*1024*1024*1024;
struct PackageOptions {
    std::vector<std::string> groups, symbols;
    int64_t start=std::numeric_limits<int64_t>::min(), end=std::numeric_limits<int64_t>::max();
    uint64_t max_file_size=default_max_file_size;
    size_t jobs=4;
    std::function<void(const std::string&)> progress;
};
Json package(const std::filesystem::path& source,const std::filesystem::path& destination,const PackageOptions& options={});
Json dataset_meta(const std::filesystem::path& root);
void set_dataset_meta(const std::filesystem::path& root,const std::string& key,const std::string& value);
struct MergeOptions {
    std::vector<std::string> groups, symbols;
    std::function<void(const std::string&)> progress;
};
Json merge_dataset(const std::filesystem::path& target,const std::filesystem::path& source,const MergeOptions& options={});
struct CompactOptions {
    uint64_t max_file_size=0;
    std::optional<int64_t> before;
    std::function<void(const std::string&)> progress;
};
Json compact(const std::filesystem::path& root,const CompactOptions& options={});
}
