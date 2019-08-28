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

#include <memory>
#include <string>
#include <thread>
#include <exception>
#include <iostream>

#include "gflags/gflags.h"

#include "backend.h"

namespace ogawayama::server {

DEFINE_string(databasename, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT


std::unique_ptr<ogawayama::common::SharedMemory> shared_memory;
static std::unique_ptr<ogawayama::common::ChannelStream> server_ch;

int backend_main(int argc, char **argv) {
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    shared_memory = std::make_unique<ogawayama::common::SharedMemory>(FLAGS_databasename, true);
    server_ch = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory->get_managed_shared_memory_ptr(), true);

    while(true) {
        ogawayama::common::ChannelMessage message;
        try {
            server_ch->get_binary_iarchive() >> message;
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return -1;
        }

        if (message.get_type() == ogawayama::common::ChannelMessage::Type::CONNECT) {
            try {
                std::thread t1(worker_main, shared_memory.get(), message.get_ivalue());
                t1.join();
            } catch (std::exception &ex) {
                std::cerr << ex.what() << std::endl;
            }
        }
    }
}

}  // ogawayama::server
