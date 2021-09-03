# Copyright 2019-2021 tsurugi fin project.
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
  "${PROJECT_SOURCE_DIR}/src/ipc*.cpp"
)

set_source_files_properties(
  ${TEST_SOURCES}
  PROPERTIES GENERATED TRUE
  COMPILE_FLAGS -Wno-unused-parameter
)

register_tests(
    TARGET tateyama_test
    DEPENDS
      server-impl
      glog::glog
      rt
    SOURCES ${TEST_SOURCES}
)