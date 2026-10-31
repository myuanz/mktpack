include("${CMAKE_CURRENT_LIST_DIR}/CPM.cmake")
set(CPM_DOWNLOAD_ALL ON)

CPMAddPackage(
  NAME zstd
  VERSION 1.5.7
  URL https://github.com/facebook/zstd/archive/refs/tags/v1.5.7.tar.gz
  URL_HASH SHA256=37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3
  SOURCE_SUBDIR build/cmake
  EXCLUDE_FROM_ALL YES
  OPTIONS "ZSTD_BUILD_SHARED OFF" "ZSTD_BUILD_STATIC ON" "ZSTD_BUILD_PROGRAMS OFF" "ZSTD_BUILD_TESTS OFF" "ZSTD_LEGACY_SUPPORT OFF" "ZSTD_MULTITHREAD_SUPPORT OFF"
)

CPMAddPackage(
  NAME nlohmann_json
  VERSION 3.12.0
  URL https://github.com/nlohmann/json/archive/refs/tags/v3.12.0.tar.gz
  URL_HASH SHA256=4b92eb0c06d10683f7447ce9406cb97cd4b453be18d7279320f7b2f025c10187
  EXCLUDE_FROM_ALL YES
  OPTIONS "JSON_BuildTests OFF" "JSON_Install OFF"
)

CPMAddPackage(
  NAME sqlite
  VERSION 3.54.1
  URL https://www.sqlite.org/2026/sqlite-amalgamation-3540100.zip
  URL_HASH SHA256=0a9c8e34352dad46006aa82836d5383bff9dc6bc9ce5a60553c05aa30e6ad1f9
  DOWNLOAD_ONLY YES
)
add_library(mktpack_sqlite STATIC "${sqlite_SOURCE_DIR}/sqlite3.c")
target_include_directories(mktpack_sqlite PUBLIC "${sqlite_SOURCE_DIR}")
target_compile_definitions(mktpack_sqlite PRIVATE SQLITE_OMIT_LOAD_EXTENSION SQLITE_THREADSAFE=1)
