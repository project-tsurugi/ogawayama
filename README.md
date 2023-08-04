# ogawayama PostgreSQL FDW interface

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* access to umikongo and manager/metadata-manager by NEC
* and see *Dockerfile* sectioni

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-system-dev libboost-thread-dev libboost-serialization-dev libgoogle-glog-dev libgflags-dev
```

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-14`

## How to build

```sh
git clone git@github.com:project-tsurugi/jogasaki.git third_party/temp_jogasaki
cd third_party/temp_jogasaki
mkdir build
cd build
cmake -DINSTALL_API_ONLY=ON ..
cmake --build . --target install
cd ../../..
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . --target install
```

available options:
* `-DBUILD_STUB_ONLY=ON` - build the stub only
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DINSTALL_EXAMPLES=ON` - also install example programs
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* `-DSHARKSFIN_IMPLEMENTATION=<implementation name>` - switch sharksfin implementation. Available options are `memory`, `mock`, `foedus-bridge`, and `kvs` (default: `memory`)
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
