/*
 * Copyright 2018-2018 umikongo project.
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

#include <errno.h>

int main(int argc, char** argv) {
    // first consume command line options for gtest
    ::testing::InitGoogleTest(&argc, argv);
#if 0
    int pid;

    if ((pid = fork()) == 0) {  // child process, execute only at build directory.
        auto retv = execlp("server/src/ogawayama-server", "ogawayama-server", nullptr);
        if (retv != 0) perror("error in ogawayama-server ");
        _exit(0);
    }
    sleep(1);
#endif
    auto retv = RUN_ALL_TESTS();
#if 0
    StubPtr stub;
    stub = make_stub();

    stub->get_impl()->get_channel()->get_binary_oarchive() <<
        ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::TERMINATE);
#endif
    return retv;
}
