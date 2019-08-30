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
#include <iostream>

#include "gflags/gflags.h"

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/stub/api.h"
#include "stubImpl.h"

DEFINE_bool(terminate, false, "terminate commnand");  // NOLINT

int main(int argc, char **argv) {
    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    StubPtr stub = make_stub(ogawayama::common::param::SHARED_MEMORY_NAME);

    if (FLAGS_terminate) {
        stub->get_impl()->get_channel()->get_binary_oarchive() <<
            ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::TERMINATE);
    }
    return 0;
}
