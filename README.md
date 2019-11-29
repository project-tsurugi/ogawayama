# ogawayama PostgreSQL FDW interface

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`
* access to umikongo
* and see *Dockerfile* sectioni

```sh
# retrieve third party modules
git submodule update --init --recursive
```

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-system-dev libboost-thread-dev libboost-serialization-dev libgoogle-glog-dev libgflags-dev
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DBUILD_STUB_ONLY=ON` - build the stub only
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DFORCE_INSTALL_RPATH=ON` - force set RPATH for non-default library paths
* `-DINSTALL_EXAMPLES=ON` - also install example programs
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
