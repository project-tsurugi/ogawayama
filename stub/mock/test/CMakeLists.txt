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

file(GLOB TEST_SOURCES
    "*.cpp"
)

# set runtime directory
# set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# declare GTEST_ROOT
set(GTEST_ROOT ../../../third_party/googletest/googletest/)

# Include files directory
include_directories(${GTEST_ROOT}/include/)

# Library directory
# link_directories(${GTEST_ROOT}/build/)

# Generate executable file
# add_executable(Test ${CMAKE_CURRENT_SOURCE_DIR}/test.cc)

# Link libraries
# target_link_libraries(stub-mock-test gtest gtest_main pthread)

register_tests(
    TARGET stub-mock
    DEPENDS
      stub-mock-test
      glog::glog
      Boost::serialization
      rt
    SOURCES ${TEST_SOURCES}
)
