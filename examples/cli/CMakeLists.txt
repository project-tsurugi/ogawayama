# Copyright 2019-2019 tsurugi project.
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

file(GLOB SOURCES
    "*.cpp"
)

add_executable(cli
    ${SOURCES}
)

set_target_properties(cli
    PROPERTIES
        RUNTIME_OUTPUT_NAME "ogawayama-cli"
)

target_include_directories(cli
    PRIVATE ${CMAKE_SOURCE_DIR}/common/include
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src/ogawayama/stub
    PRIVATE ${CMAKE_BINARY_DIR}/src
)

target_link_libraries(cli
    PRIVATE stub
    PRIVATE message-manager-interface
    PRIVATE gflags::gflags
    PRIVATE rt
)

install_custom(cli ${export_name})
