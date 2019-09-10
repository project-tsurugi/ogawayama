# ogawayama PostgreSQL FDW interface

## Requirements

* CMake `>= 3.5`
* C++ Compiler `>= C++17`

```
RUN apt update -y && apt install -y git build-essential cmake ninja-build libboost-system-dev libboost-serialization-dev libgflags-dev
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* `-DBUILD_TESTS=OFF` - never build test programs
* `-DCMAKE_PREFIX_PATH=<installation directory>` - indicate prerequiste installation directory
* for debugging only
  * `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  * `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  * `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
