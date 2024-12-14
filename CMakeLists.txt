# Copyright 2019-2024 Project Tsurugi.
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

cmake_minimum_required(VERSION 3.16)

project(ogawayama
    VERSION 1.0.0
    DESCRIPTION "PostgreSQL FDW interface"
    LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)
option(BUILD_TESTS "Build test programs" ON)
option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)
option(BUILD_STUB_ONLY "build the stub only" OFF)
option(BUILD_BRIDGE_ONLY "build the bridge only" OFF)

if(NOT DEFINED SHARKSFIN_IMPLEMENTATION)
    set(
            SHARKSFIN_IMPLEMENTATION "shirakami"
            CACHE STRING
            "sharksfin target name to link"
            FORCE
    )
endif()

find_package(Threads REQUIRED)
find_package(Boost
        COMPONENTS filesystem thread system container serialization
        REQUIRED
)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)
if(NOT BUILD_BRIDGE_ONLY)
find_package(metadata-manager REQUIRED)
find_package(message-manager REQUIRED)
find_package(Protobuf REQUIRED)
endif()
if(NOT BUILD_STUB_ONLY)
find_package(yugawara REQUIRED)
find_package(jogasaki-${SHARKSFIN_IMPLEMENTATION} REQUIRED)
find_package(tateyama-${SHARKSFIN_IMPLEMENTATION} REQUIRED)
find_package(sharksfin REQUIRED)
endif()
find_package(takatori REQUIRED)
find_package(mpdecpp REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(Tests)

if (BUILD_TESTS)
    enable_testing()
endif()

add_library(api INTERFACE)

target_include_directories(api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/ogawayama>
)
      
add_library(bridge-api INTERFACE)

target_include_directories(bridge-api
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/bridge/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/ogawayama>
)

set(export_name "ogawayama-${SHARKSFIN_IMPLEMENTATION}")
set(package_name "ogawayama-${SHARKSFIN_IMPLEMENTATION}")

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install_custom(api ${export_name})
install_custom(bridge-api ${export_name})

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

install(
    EXPORT ${package_name}
    NAMESPACE ${package_name}-
    FILE ${package_name}-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
    EXPORT_LINK_INTERFACE_LIBRARIES
)

if(NOT BUILD_BRIDGE_ONLY)
    add_subdirectory(src)
if(BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()
endif()
if(NOT BUILD_STUB_ONLY)
    add_subdirectory(bridge)
endif()
if(BUILD_TESTS)
    add_subdirectory(third_party)
    add_subdirectory(test)
endif()
