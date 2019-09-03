# Copyright 2018-2018 umikongo project.
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

add_executable(prepare
    ${SOURCES}
)

set_target_properties(prepare
    PROPERTIES
        RUNTIME_OUTPUT_NAME "ogawayama-prepare"
)

target_include_directories(prepare
    PRIVATE .
    PRIVATE ../../stub/include
    PRIVATE ../../common/include
)

target_link_libraries(prepare
    PRIVATE stub
    PRIVATE Boost::serialization
    PRIVATE pthread
    PRIVATE rt
)

set_compile_options(prepare)

if(INSTALL_EXAMPLES)
    install_custom(prepare ${export_name})
endif()
