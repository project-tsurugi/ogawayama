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

#include <boost/asio.hpp>
#include "boost/asio/io_service.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "worker.h"
#include "SignalHandler.h"
#include "utils.h"

#include "server.h"

namespace ogawayama::server {

DEFINE_string(dbname, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT

static constexpr std::string_view KEY_LOCATION { "location" };  //NOLINT

int backend_main(int argc, char **argv) {
    // database environment
    auto env = umikongo::create_environment();
    env->initialize();

    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // database
    auto db = umikongo::create_database();
    std::map<std::string, std::string> options;
    options.insert_or_assign(std::string(KEY_LOCATION), FLAGS_location);
    db->open(options);

    // communication channel
    std::unique_ptr<ogawayama::common::SharedMemory> shared_memory;
    std::unique_ptr<ogawayama::common::ChannelStream> server_ch;
    try {
        shared_memory = std::make_unique<ogawayama::common::SharedMemory>(FLAGS_dbname, true, FLAGS_remove_shm);
        server_ch = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory.get(), true, false);
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
        return -1;
    }

    boost::asio::io_service ios;
    SignalHandler signal_handler{[&server_ch](){
            server_ch->send_req(ogawayama::common::CommandMessage::Type::TERMINATE);
    }};

    std::vector<std::unique_ptr<Worker>> workers;
    while(true) {
        ogawayama::common::CommandMessage::Type type;
        std::size_t index;
        try {
            auto rv = server_ch->recv_req(type, index);
            if (rv != ERROR_CODE::OK) {
                if (rv != ERROR_CODE::TIMEOUT) {
                    std::cerr << __func__ << " " << __LINE__ <<  " " << ogawayama::stub::error_name(rv) << std::endl;
                }
                continue;
            }
            server_ch->notify();
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return -1;
        }

        switch (type) {
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
                dump(db.get(), FLAGS_location);
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
            break;
        case ogawayama::common::CommandMessage::Type::LOAD_DATABASE:
            try {
                load(db.get(), FLAGS_location);
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
    signal_handler.shutdown();
}
    
}  // ogawayama::server
