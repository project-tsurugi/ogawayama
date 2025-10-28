# ogawayama PostgreSQL FDW interface

## Requirements

* CMake `>= 3.16`
* C++ Compiler `>= C++17`
* access to jogasaki and manager/metadata-manager
* and see *Dockerfile* sectioni

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-system-dev libboost-thread-dev libboost-serialization-dev libgoogle-glog-dev libgflags-dev protobuf-compiler protobuf-c-compiler libprotobuf-dev
```

optional packages:

* `doxygen`
* `graphviz`
* `clang-tidy-14`

### Install modules

#### tsurugidb modules

This requires below [tsurugidb](https://github.com/project-tsurugi/tsurugidb) modules to be installed.

* [metadata-manager](https://github.com/project-tsurugi/metadata-manager)
* [message-manager](https://github.com/project-tsurugi/message-manager)

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
* `-DBUILD_BRIDGE_ONLY=ON` - build the bridge only
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DBUILD_EXAMPLES=ON` - also build example programs
* `-DBUILD_STRICT=OFF` - don't treat compile warnings as build errors
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* `-DSHARKSFIN_IMPLEMENTATION=<implementation name>` - switch sharksfin implementation. Available options are `memory` and `shirakami` (default: `shirakami`)
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
