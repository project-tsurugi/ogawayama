# Copyright 2019-2025 tsurugi fin project.
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

add_library(stub-mock
    ${SOURCES}
)

target_include_directories(stub-mock
    PRIVATE ../../include
    PRIVATE ../../../common/include
)

set_target_properties(stub-mock
    PROPERTIES
        INSTALL_RPATH "\$ORIGIN"
        LIBRARY_OUTPUT_NAME "ogawayama-stub-mock"
)

target_link_libraries(stub-mock
    PRIVATE message-manager-interface
)

#for tests
add_library(stub-mock-impl INTERFACE)

target_include_directories(stub-mock-impl
    INTERFACE ../../include
    INTERFACE ../../../common/include
)
