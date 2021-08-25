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

file(GLOB SOURCES
    "*.cpp"
)

set(GENERATED_PROTO_SRCS
    ${CMAKE_BINARY_DIR}/server/protos/request.pb.cc
    ${CMAKE_BINARY_DIR}/server/protos/response.pb.cc
    ${CMAKE_BINARY_DIR}/server/protos/common.pb.cc
    ${CMAKE_BINARY_DIR}/server/protos/schema.pb.cc
)

set_source_files_properties(
    ${GENERATED_PROTO_SRCS}
    PROPERTIES GENERATED TRUE
    COMPILE_FLAGS -Wno-unused-parameter
)

add_executable(server
    ${SOURCES}
    ${GENERATED_PROTO_SRCS}
)

set_target_properties(server
    PROPERTIES
        RUNTIME_OUTPUT_NAME "ogawayama-server"
)

add_dependencies(server
    build_protos
)

target_include_directories(server
    PRIVATE ${PG_INCLUDEDIR}
    PRIVATE ${PROTOBUF_INCLUDE_DIRS}
    PRIVATE ${CMAKE_BINARY_DIR}/server/protos
)

target_link_libraries(server
    PRIVATE jogasaki-${SHARKSFIN_IMPLEMENTATION}-engine
    PRIVATE takatori
    PRIVATE yugawara
    PRIVATE fpdecimal
    PRIVATE shakujo-common
    PRIVATE manager-metadata
    PRIVATE manager-message-interface
    PRIVATE gflags::gflags
    PRIVATE glog::glog
    PRIVATE Boost::filesystem
    PRIVATE Boost::thread
    PRIVATE pthread
    PRIVATE rt
    PRIVATE msgpackc
    PRIVATE ${PROTOBUF_LIBRARY}
)

set_compile_options(server)

install_custom(server ${export_name})
