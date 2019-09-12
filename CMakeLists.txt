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

cmake_minimum_required(VERSION 3.5)

project(ogawayama
    VERSION 0.0.1
    DESCRIPTION "PostgreSQL FDW interface"
    LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

option(BUILD_SHARED_LIBS "build shared libraries instead of static" ON)
option(BUILD_TESTS "Build test programs" ON)
option(FORCE_INSTALL_RPATH "Force add lib dir of custom prefixes to INSTALL_RPATH" OFF)

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

find_package(Threads REQUIRED)
find_package(Boost
        COMPONENTS system filesystem serialization
        REQUIRED
)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)
include(InstallOptions)
include(Tests)

if (BUILD_TESTS)
    enable_testing()
endif()

set(export_name "ogawayama")
set(package_name "ogawayama")

configure_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
    @ONLY
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${package_name}-config-version.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${package_name}
)

add_subdirectory(stub)
add_subdirectory(server)
add_subdirectory(examples)
add_subdirectory(third_party)
