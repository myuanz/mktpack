sync:
    uv sync

build:
    cmake -S . -B build/cli -DMKTPACK_BUILD_PYTHON=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build build/cli -j4

test: build
    uv run pytest -q
    ctest --test-dir build/cli --output-on-failure

wheel: manylinux-whl win-whl

manylinux-whl:
    #!/usr/bin/env bash
    set -euo pipefail
    out="$PWD/dist"
    rm -f "$out"/*manylinux*.whl "$out"/*.tar.gz
    uv build --sdist
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    tar -xzf "$out"/mktpack-*.tar.gz -C "$tmp"
    cd "$tmp"/mktpack-*
    CPM_SOURCE_CACHE="${CPM_SOURCE_CACHE:-$HOME/.cache/CPM}" uvx cibuildwheel --platform linux --output-dir "$out" .

win-whl:
    #!/usr/bin/env bash
    set -euo pipefail
    win_py="${MKTPACK_WINDOWS_PYTHON:-$HOME/.cache/mktpack-win-python312/tools}"
    if [ ! -f "$win_py/libs/python3.lib" ]; then
        cache="$(dirname "$win_py")"
        mkdir -p "$cache"
        curl -fsSL "https://www.nuget.org/api/v2/package/python/3.12.10" -o "$cache/python312.nupkg"
        python3 -m zipfile -e "$cache/python312.nupkg" "$cache"
    fi
    rm -rf build/win_amd64 dist/*win_amd64.whl
    _PYTHON_HOST_PLATFORM="win-amd64" \
    MKTPACK_WINDOWS_PYTHON="$win_py" \
    CPM_SOURCE_CACHE="${CPM_SOURCE_CACHE:-$HOME/.cache/CPM}" \
    uv build --python 3.12 --wheel --out-dir dist \
        -C "cmake.args=-DCMAKE_TOOLCHAIN_FILE=$PWD/cmake/windows-mingw.cmake" \
        -C "build-dir=build/win_amd64"
    uvx abi3audit --strict --report dist/*win_amd64.whl
