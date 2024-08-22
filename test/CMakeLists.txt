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

if(NOT BUILD_BRIDGE_ONLY)
file(GLOB STUB_TEST_SOURCES
    "ogawayama/stub/*Test.cpp"
)

register_tests(
    TARGET stub
    DEPENDS
      message-manager-interface
      Boost::serialization
      glog::glog
      gflags::gflags
      rt
      protobuf::libprotobuf
    SOURCES ${STUB_TEST_SOURCES}
    INCLUDES
      ${CMAKE_SOURCE_DIR}/src
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_SOURCE_DIR}/common/include
      ${CMAKE_SOURCE_DIR}/test/include
      ${CMAKE_BINARY_DIR}/src
)
endif()

if(NOT BUILD_STUB_ONLY)
file(GLOB BRIDGE_TEST_SOURCES
    "ogawayama/bridge/*Test.cpp"
    "ogawayama/bridge/database_mock.cpp"
)

register_tests(
    TARGET bridge
    DEPENDS
      jogasaki-${SHARKSFIN_IMPLEMENTATION}-engine
      Boost::filesystem
      Boost::serialization
      glog::glog
      gflags::gflags
      rt
    SOURCES ${BRIDGE_TEST_SOURCES}
    INCLUDES
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_SOURCE_DIR}/common/include
      ${CMAKE_SOURCE_DIR}/bridge/src
      ${CMAKE_SOURCE_DIR}/bridge/include
      ${CMAKE_SOURCE_DIR}/third_party/metadata-manager/include
)
endif()
