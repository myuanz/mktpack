set(CMAKE_SYSTEM_NAME Windows)
set(CMAKE_SYSTEM_PROCESSOR x86_64)
set(CMAKE_C_COMPILER x86_64-w64-mingw32-gcc)
set(CMAKE_CXX_COMPILER x86_64-w64-mingw32-g++)
set(CMAKE_RC_COMPILER x86_64-w64-mingw32-windres)

if(NOT "$ENV{MKTPACK_WINDOWS_PYTHON}" STREQUAL "")
  set(Python_INCLUDE_DIR "$ENV{MKTPACK_WINDOWS_PYTHON}/include" CACHE PATH "目标 CPython 头文件")
  set(Python_LIBRARY "$ENV{MKTPACK_WINDOWS_PYTHON}/libs/python312.lib" CACHE FILEPATH "目标 CPython 导入库")
  set(Python_SABI_LIBRARY "$ENV{MKTPACK_WINDOWS_PYTHON}/libs/python3.lib" CACHE FILEPATH "目标 CPython 稳定 ABI 导入库")
  # FindPython 的解释器运行于构建机；扩展名由目标系统决定。
  set(NB_SUFFIX_S ".pyd")
endif()
