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

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
