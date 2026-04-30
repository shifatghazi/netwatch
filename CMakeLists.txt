cmake_minimum_required(VERSION 3.16)
project(netwatch VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Build type defaults to Release for production, Debug for development
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
# -Wall -Wextra: enable all warnings — treat warnings as errors in a real project
# -O2: optimize for speed without sacrificing debuggability too much
# -g:  include debug symbols even in release (for post-mortem debugging)
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g -fsanitize=address,undefined")

add_executable(netwatch_engine
    src/engine.cpp
)

target_include_directories(netwatch_engine PRIVATE
    src/
    third_party/  # nlohmann/json lives here
)

# Link against inotify — it's part of glibc on Linux, no separate package needed
# On macOS you'd need a portability shim or different approach

install(TARGETS netwatch_engine DESTINATION bin)
