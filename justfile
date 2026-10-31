sync:
    uv sync

build:
    cmake -S . -B build/cli -DMKTPACK_BUILD_PYTHON=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build build/cli -j4

test: build
    uv run pytest -q
    ctest --test-dir build/cli --output-on-failure

wheel:
    uv build
