# Repository Guidelines

## 项目状态与依据

MktPack 是金融时序数据格式与读写库，包名为 `mktpack`，CLI 为 `mkt`，文件扩展名为 `.mkp`。当前仓库处于规划阶段，开发前阅读 `实施计划.md`；以下源码布局和构建入口随实施建立。需求与字节格式分别维护在 `docs/需求.md`、`docs/规范.md`，README 保留用途与基本用法。

## 模块组织

- `cpp/core`、`include/mktpack`：schema、整数编解码、单文件读写
- `cpp/storage`：数据集查询、SQLite 索引、合并与分卷
- `cpp/port`：数据源适配器，币安下载及字段映射放在 `binance.cpp`
- `cpp/cli`：命令参数和终端进度；`cpp/bindings`、`python/mktpack`：薄绑定与 Arrow 包装
- `tests`：自动测试；`validation`：只读真实样本和验收记录

通用存储调用核心，适配器调用存储。主要功能使用 C++20，交接层以外的 C++ 不依赖 Python；Python 模块不包含下载器逻辑。

## 构建与开发命令

使用 Python 3.12+ 和 uv 管理环境。构建采用 CMake、CPM 固定版本依赖、nanobind 和 scikit-build-core。目前尚无构建配置；建立后使用以下入口，并在 justfile 中封装常用操作：

| 命令 | 用途 |
|---|---|
| `uv sync` | 同步开发环境 |
| `cmake -S . -B build` | 配置 C++ 构建 |
| `cmake --build build` | 编译库与 CLI |
| `ctest --test-dir build --output-on-failure` | 运行 C++ 测试 |
| `uv run pytest -q` | 运行 Python 接口测试 |
| `uv build` | 构建本地分发包 |

CLI 建成后的本地检查示例：`mkt --root binance-data binance check`。

## 编码与命名

交流、文档和注释使用中文。新代码使用四空格缩进，修改已有代码时遵循所在文件风格。Python 函数和变量使用 `snake_case`，公开接口添加类型标注，文档字符串使用三单引号。使用 `uv add` 管理依赖，不使用 pip。

保持职责集中，优先使用已有库；避免重复校验和无依据的限制。不运行自动格式化、ruff 或 py_compile

## 测试与验收

采用 CTest 和 pytest，Python 测试文件命名为 `test_*.py`。按改动覆盖精确数值往返、最高小数精度合并、时间区间 `[start,end)`、重复时间顺序，以及目录与打包结果的一致性。下载测试覆盖增量跳过与全量补齐。

真实样本保持只读，验收记录来源、字段映射、行数、体积和耗时。生成数据放入已忽略的 `validation/generated/` 或 `validation/output/`。运行受影响的测试并报告结果。

## 提交与拉取请求

当前历史只有 `init`，尚无固定提交前缀。提交说明简洁描述具体变化，每项交付验收后提交。PR 说明问题、最终行为及验证结果；有关联 issue 时附链接，涉及格式或接口变化时同步对应规范。提交前检查 diff，不提交本地环境、构建产物或下载数据。
