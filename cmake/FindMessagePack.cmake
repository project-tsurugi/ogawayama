# Copyright 2018-2018 shark's fin project.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(TARGET msgpack-cpp)
    return()
endif()

find_path(MessagePack_INCLUDE_DIR NAMES msgpack.hpp)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MessagePack DEFAULT_MSG
    MessagePack_INCLUDE_DIR)

if(MessagePack_INCLUDE_DIR)
    set(MessagePack_FOUND ON)
    add_library(msgpack-cpp INTERFACE IMPORTED)
    set_target_properties(msgpack-cpp PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${MessagePack_INCLUDE_DIR}")
else()
    set(MessagePack_FOUND OFF)
endif()

unset(MessagePack_INCLUDE_DIR CACHE)
