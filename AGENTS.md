# Repository Guidelines

## 项目状态与依据

MktPack 是金融时序数据格式与读写库，包名为 `mktpack`，CLI 为 `mkt`，文件扩展名为 `.mkp`。基础格式、数据集查询和币安下载已实现。开发前阅读 `实施计划.md`、`docs/需求.md` 和 `docs/规范.md`，README 保留用途与基本用法。

## 模块组织

- `cpp/core`、`include/mktpack`：schema、整数编解码、单文件读写
- `cpp/storage`：数据集查询、SQLite 索引、合并与分卷
- `cpp/port`：数据源适配器，币安下载及字段映射放在 `binance.cpp`，不要把某个 port 的机制侵入全局代码
- `cpp/cli`：命令参数和终端进度；`cpp/bindings`、`python/mktpack`：薄绑定与 Arrow 包装
- `tests`：自动测试；`validation`：只读真实样本和验收记录

通用存储调用核心，适配器调用存储。所有 CLI 均由原生 C++ 执行，交接层以外的 C++ 不依赖 Python。Python 仅负责 wheel 分发、数据读取与 Arrow table 包装、raw 接口。
开发阶段直接修改接口，不保留兼容转发或旧入口别名。
MKP 时序、SQLite 时序、SQLite 普通表按组描述读写，具体间实施计划；

core、storage、package 相关逻辑不应被 port 业务侵入

## 构建与开发命令

使用 Python 3.12+ 和 uv 管理环境。构建采用 CMake、CPM 固定版本依赖、nanobind 和 scikit-build-core，常用入口如下：

| 命令 | 用途 |
|---|---|
| `uv sync` | 同步开发环境 |
| `just build` | 配置并编译独立 C++ 库与 CLI |
| `just test` | 构建并运行 Python、C++ 测试 |
| `ctest --test-dir build/cli --output-on-failure` | 运行 C++ 测试 |
| `uv run pytest -q` | 运行 Python 接口测试 |
| `uv build` | 构建本地分发包 |

本地检查示例：`mkt --root data/binance binance check`。

## 编码与命名

交流、文档和注释使用中文。新代码使用四空格缩进，修改已有代码时遵循所在文件风格。Python 函数和变量使用 `snake_case`，公开接口添加类型标注，文档字符串使用三单引号。使用 `uv add` 管理依赖，不使用 pip。

保持职责集中，优先使用已有库；避免重复校验和无依据的限制。不运行自动格式化、ruff 或 py_compile.

系统表使用 `__名称__`，适配器表按具体来源区分，如 `__binance_datavision_`、`__binance_rest_`

## 空间约束

品种、历史长度、重复增量运行等持续增长的东西应着重考虑其空间占用，例如组与品种分别建字典

## 测试与验收

采用 CTest 和 pytest，Python 测试文件命名为 `test_*.py`。按改动覆盖精确数值往返、最高小数精度合并、时间区间 `[start,end)`、重复时间顺序，以及目录与打包结果的一致性。下载测试覆盖增量跳过与全量补齐。

真实样本保持只读，验收记录来源、字段映射、行数、体积和耗时。生成数据放入已忽略的 `validation/generated/` 或 `validation/output/`。运行受影响的测试并报告结果。
持续验证写入 `tests/`

## 提交与拉取请求

提交历史采用简短的变化描述，不强制固定前缀。未经用户明确要求不提交。PR 说明问题、最终行为及验证结果；有关联 issue 时附链接，涉及格式或接口变化时同步对应规范。提交前检查 diff，不提交本地环境、构建产物或下载数据。
