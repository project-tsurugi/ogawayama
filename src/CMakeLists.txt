# Copyright 2019-2023 tsurugi fin project.
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

set(SqlProtoFiles
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/common.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/request.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/response.proto
        ${CMAKE_SOURCE_DIR}/src/jogasaki/proto/sql/error.proto
        )
set(FrameworkProtoFiles
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/framework/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/framework/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/framework/common.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/diagnostics.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/core/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/core/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/endpoint/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/endpoint/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/auth/request.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/auth/response.proto
        ${CMAKE_SOURCE_DIR}/src/tateyama/proto/auth/common.proto
        )

# By default, PROTOBUF_GENERATE_CPP generates file path for .pb.cc as if they are in the same directory.
# Work-around this with PROTOBUF_GENERATE_CPP_APPEND_PATH
set(PROTOBUF_GENERATE_CPP_APPEND_PATH OFF)

PROTOBUF_GENERATE_CPP(SqlProtobufSources SqlProtobufHeaders ${SqlProtoFiles})
PROTOBUF_GENERATE_CPP(FrameworkProtobufSources FrameworkProtobufHeaders ${FrameworkProtoFiles})

# needed in both current and parent scopes
set(GENERATED_SQL_PROTO_SRCS ${SqlProtobufSources} PARENT_SCOPE)
set(GENERATED_SQL_PROTO_SRCS ${SqlProtobufSources})
set(GENERATED_FRAMEWORK_PROTO_SRCS ${FrameworkProtobufSources} PARENT_SCOPE)
set(GENERATED_FRAMEWORK_PROTO_SRCS ${FrameworkProtobufSources})

add_custom_target(build_protos
        DEPENDS
        ${SqlProtobufSources}
        ${SqlProtoFiles}
        ${FrameworkProtobufSources}
        ${FrameworkProtoFiles}
)

file(GLOB SOURCES
    "ogawayama/stub/*.cpp"
    "tateyama/utils/*.cpp"
    "jogasaki/serializer/*.cpp"
    "jogasaki/utils/*.cpp"
)

set_source_files_properties(
    ${GENERATED_SQL_PROTO_SRCS}
    ${GENERATED_FRAMEWORK_PROTO_SRCS}
    PROPERTIES
    GENERATED TRUE
    COMPILE_FLAGS -Wno-unused-parameter
)

add_library(stub
       ${SOURCES}
       ${GENERATED_SQL_PROTO_SRCS}
       ${GENERATED_FRAMEWORK_PROTO_SRCS}
)

add_dependencies(stub
  build_protos
)

set_target_properties(stub
    PROPERTIES
        INSTALL_RPATH "\$ORIGIN/../lib"
        LIBRARY_OUTPUT_NAME "ogawayama-stub"
)

target_include_directories(stub
    PRIVATE ${CMAKE_SOURCE_DIR}/include
    PRIVATE ${CMAKE_SOURCE_DIR}/common/include
    PRIVATE ${CMAKE_SOURCE_DIR}/src
    PRIVATE ${CMAKE_BINARY_DIR}/src
)

target_link_libraries(stub
    PUBLIC takatori
    PRIVATE metadata-manager-api
    PRIVATE message-manager-interface
    PRIVATE Boost::serialization
    PRIVATE pthread
    PRIVATE rt
    PRIVATE protobuf::libprotobuf
)

install_custom(stub ${export_name})

#for tests
add_library(stub-impl INTERFACE)

target_include_directories(stub-impl
    INTERFACE ${CMAKE_SOURCE_DIR}/include
    INTERFACE ${CMAKE_SOURCE_DIR}/common/include
    INTERFACE ${CMAKE_SOURCE_DIR}/src
    INTERFACE ${CMAKE_BINARY_DIR}/src
    INTERFACE ogawayama/stub
)
