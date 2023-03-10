# Copyright 2019-2020 tsurugi project.
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

add_executable(tpcc
    ${SOURCES}
)

set_target_properties(tpcc
    PROPERTIES
        RUNTIME_OUTPUT_NAME "ogawayama-tpcc"
)

target_include_directories(tpcc
    PRIVATE .
    PRIVATE ${CMAKE_SOURCE_DIR}/common/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(tpcc
    PUBLIC stub
    PRIVATE message-manager-interface
    PRIVATE gflags::gflags
    PRIVATE glog::glog
    PRIVATE Boost::thread
    PRIVATE pthread
    PRIVATE rt
)

set_compile_options(tpcc)

if(INSTALL_EXAMPLES)
    install_custom(tpcc ${export_name})
endif()

# add_test(
#         NAME tpcc
#         COMMAND tpcc
# )
