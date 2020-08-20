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

if(NOT DEFINED SHARKSFIN_IMPLEMENTATION)
    set(
        SHARKSFIN_IMPLEMENTATION "memory"
        CACHE STRING
        "sharksfin target name to link"
        FORCE
    )
endif()

find_package(umikongo-${SHARKSFIN_IMPLEMENTATION} REQUIRED)
find_package(shakujo REQUIRED)

file(GLOB SOURCES
    "*.cpp"
)

add_executable(server
    ${SOURCES}
)

set_target_properties(server
    PROPERTIES
        RUNTIME_OUTPUT_NAME "ogawayama-server"
)

target_include_directories(server
    PRIVATE ../../stub/include
    PRIVATE ../../common/include
)

target_link_libraries(server
    PRIVATE umikongo-${SHARKSFIN_IMPLEMENTATION}-executor-${SHARKSFIN_IMPLEMENTATION}
    PRIVATE shakujo-common
    PRIVATE manager-metadata
    PRIVATE manager-message-interface
    PRIVATE gflags::gflags
    PRIVATE glog::glog
    PRIVATE Boost::filesystem
    PRIVATE Boost::thread
    PRIVATE pthread
    PRIVATE rt
)

install_custom(server ${export_name})
