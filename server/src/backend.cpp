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
#include <string>

#include "gflags/gflags.h"

#include "ogawayama/common/channel_stream.h"

namespace ogawayama::server {

DEFINE_string(databasename, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT

int backend_main(int argc, char **argv) {
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    boost::interprocess::shared_memory_object::remove(FLAGS_databasename.c_str());
    boost::interprocess::managed_shared_memory *mem = new boost::interprocess::managed_shared_memory(boost::interprocess::create_only, FLAGS_databasename.c_str(), ogawayama::common::param::SEGMENT_SIZE);
    assert (mem->get_size() == ogawayama::common::param::SEGMENT_SIZE);

    ogawayama::common::ChannelStream server_ch(ogawayama::common::param::server, mem, true);
    boost::archive::binary_iarchive server_ia(server_ch);
    std::string command;

    server_ia >> command;

    while(true) {

    }
}

}  // ogawayama::server
