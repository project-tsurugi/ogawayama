# Copyright 2019-2021 tsurugi project.
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

add_executable(terminater
    ${SOURCES}
)

set_target_properties(terminater
    PROPERTIES
        RUNTIME_OUTPUT_NAME "tateyama-terminater"
)

target_link_libraries(terminater
    PRIVATE jogasaki-${SHARKSFIN_IMPLEMENTATION}-engine
    PRIVATE gflags::gflags
    PRIVATE glog::glog
    PRIVATE pthread
    PRIVATE rt
)

target_include_directories(terminater
    PRIVATE ../src
)

set_compile_options(terminater)

install_custom(terminater ${export_name})
