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

add_library(bridge
    ${SOURCES}
)

set_target_properties(bridge PROPERTIES
    OUTPUT_NAME "ogawayama-bridge-${SHARKSFIN_IMPLEMENTATION}"
)

target_include_directories(bridge
    PRIVATE ../../common/include
)

target_link_libraries(bridge
    PUBLIC api
    PUBLIC bridge-api
    PRIVATE jogasaki-${SHARKSFIN_IMPLEMENTATION}-api
    PRIVATE tateyama-${SHARKSFIN_IMPLEMENTATION}-engine
    PRIVATE takatori
    PRIVATE yugawara
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

install_custom(bridge ${export_name})

#for tests
add_library(bridge-impl INTERFACE)

target_include_directories(bridge-impl
    INTERFACE ../include
    INTERFACE .
)
