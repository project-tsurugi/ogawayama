/*
 * Copyright 2019-2019 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "TestRoot.h"

#include <stdlib.h>
#include <errno.h>

#include <gflags/gflags.h>

namespace ogawayama::bridge {
DEFINE_bool(remove_shm, true, "remove the shared memory prior to the execution");  // NOLINT
}

int main(int argc, char **argv) {
    google::InitGoogleLogging("ogawayama tests");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
