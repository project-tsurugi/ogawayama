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
#include "boost/archive/binary_iarchive.hpp"

#include "ogawayama/common/channel_stream.h"


namespace ogawayama {
namespace server {

const std::size_t SEGMENT_SIZE = 100<<20; // 100 MiB (tantative)
const char* SHARED_MEMORY_NAME = "ogawayama"; // (tantative)

int backend_main(int argc, char **argv) {
    boost::interprocess::managed_shared_memory *mem;
    mem = new boost::interprocess::managed_shared_memory(boost::interprocess::open_only, SHARED_MEMORY_NAME);
    assert (mem->get_size() == SEGMENT_SIZE);

    ogawayama::common::ChannelStream server_ch("Server", mem);
    boost::archive::binary_iarchive server_ia(server_ch);
    std::string command;

    server_ia >> command;

    while(true) {

    }
}

}  // server
}  // ogawayama
