# Copyright 2019-2019 tsurugi fin project.
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

add_library(stub
    ${SOURCES}
)

set_target_properties(stub
    PROPERTIES
        LIBRARY_OUTPUT_NAME "ogawayama-stub"
)

target_include_directories(stub
    PRIVATE ../include
    PRIVATE ../../common/include
)

target_link_libraries(stub
    PRIVATE manager-message-interface
    PRIVATE pthread
    PRIVATE rt
)

install_custom(stub ${export_name})

#for tests
add_library(stub-impl INTERFACE)

target_include_directories(stub-impl
    INTERFACE ../include
    INTERFACE ../../common/include
    INTERFACE .
)
