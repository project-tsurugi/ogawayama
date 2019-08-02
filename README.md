# ogawayama PostgreSQL FDW interface

## Requirements

* CMake `>= 3.5`
* C++ Compiler `>= C++17`

RUN apt update -y && apt install -y git build-essential cmake ninja-build
```

## How to build

```sh
mkdir build
cd build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
ninja
```

available options:
* (currently nothing)

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
