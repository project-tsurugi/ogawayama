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
#include <exception>
#include <iostream>

#include "gflags/gflags.h"

#include "worker.h"
#include "utils.h"

#include "server.h"

namespace ogawayama::server {

DEFINE_string(dbname, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT

static constexpr std::string_view KEY_LOCATION { "location" };  //NOLINT

int backend_main(int argc, char **argv) {
    std::vector<std::unique_ptr<Worker>> workers;

    // database environment
    auto env = umikongo::create_environment();
    env->initialize();

    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // communication channel
    auto shared_memory = std::make_unique<ogawayama::common::SharedMemory>(FLAGS_dbname, true);
    auto server_ch = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory->get_managed_shared_memory_ptr(), true);

    // database
    auto db = umikongo::create_database();
    std::map<std::string, std::string> options;
    options.insert_or_assign(std::string(KEY_LOCATION), FLAGS_location);
    db->open(options);

    while(true) {
        ogawayama::common::CommandMessage message;
        try {
            server_ch->get_binary_iarchive() >> message;
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return -1;
        }
        std::size_t index = message.get_ivalue();

        switch (message.get_type()) {
        case ogawayama::common::CommandMessage::Type::CONNECT:
            if (workers.size() < (index + 1)) {
                workers.resize(index + 1);
            }
            try {
                std::unique_ptr<Worker> &worker = workers.at(index);
                worker = std::make_unique<Worker>(db.get(), shared_memory.get(), index);
                worker->task_ = std::packaged_task<void()>([&]{worker->run();});
                worker->future_ = worker->task_.get_future();
                worker->thread_ = std::thread(std::move(worker->task_));
            } catch (std::exception &ex) {
                std::cerr << ex.what() << std::endl;
                return -1;
            }
            break;
        case ogawayama::common::CommandMessage::Type::DUMP_DATABASE:
            try {
                load(db.get(), FLAGS_location);
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
            break;
        case ogawayama::common::CommandMessage::Type::LOAD_DATABASE:
            try {
                dump(db.get(), FLAGS_location);
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
            break;
        case ogawayama::common::CommandMessage::Type::TERMINATE:
            return 0;
        default:
            std::cerr << "unsurpported message" << std::endl;
            return -1;
        }
    }
}

}  // ogawayama::server
