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
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "terminator.h"

namespace ogawayama::server {

DEFINE_string(dbname, "tsubakuro", "database name");  // NOLINT

int terminator_main(int argc, char **argv) {
    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // terminator
    auto container = std::make_unique<tsubakuro::common::wire::connection_container>(FLAGS_dbname);
    container->get_connection_queue().request_terminate();
    
    return 0;
}

}  // ogawayama::server


int main(int argc, char **argv) {
    return ogawayama::server::terminator_main(argc, argv);
}
