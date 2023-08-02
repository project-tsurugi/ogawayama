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
)

register_tests(
    TARGET stub
    DEPENDS
      stub-impl
      bridge-impl
      message-manager-interface
      jogasaki-${SHARKSFIN_IMPLEMENTATION}-api
      tateyama-${SHARKSFIN_IMPLEMENTATION}-api
      sharksfin-api
      Boost::filesystem
      glog::glog
      gflags::gflags
      rt
      protobuf::libprotobuf
    SOURCES ${STUB_TEST_SOURCES}
)

file(GLOB BRIDGE_TEST_SOURCES
    "ogawayama/bridge/*Test.cpp"
    "ogawayama/bridge/database_mock.cpp"
)

register_tests(
    TARGET bridge
    DEPENDS
      stub-impl
      bridge-impl
      message-manager-interface
      jogasaki-${SHARKSFIN_IMPLEMENTATION}-engine
      Boost::filesystem
      glog::glog
      gflags::gflags
      rt
    SOURCES ${BRIDGE_TEST_SOURCES}
)
