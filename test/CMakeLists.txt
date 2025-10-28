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

file(GLOB STUB_TEST_SOURCES
    "ogawayama/stub/*Test.cpp"
    "${CMAKE_SOURCE_DIR}/src/takatori/util/exception.cpp"
    "${CMAKE_SOURCE_DIR}/src/takatori/util/string_builder.cpp"
    "${CMAKE_SOURCE_DIR}/src/takatori/datetime/date.cpp"
    "${CMAKE_SOURCE_DIR}/src/takatori/datetime/time_point.cpp"
    "${CMAKE_SOURCE_DIR}/src/takatori/datetime/printing.cpp"
)

register_tests(
    TARGET stub
    DEPENDS
      Boost::serialization
      glog::glog
      gflags::gflags
      rt
      protobuf::libprotobuf
      ${BOOST_STACK_TRACE}      
    SOURCES ${STUB_TEST_SOURCES}
    INCLUDES
      ${CMAKE_SOURCE_DIR}/src
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_SOURCE_DIR}/common/include
      ${CMAKE_SOURCE_DIR}/test/include
      ${CMAKE_BINARY_DIR}/src
)
