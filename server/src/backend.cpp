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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>

#include "worker.h"
#include "SignalHandler.h"
#include "utils.h"

#include "server.h"

namespace ogawayama::server {

DEFINE_string(dbname, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT
DEFINE_int32(read_batch_size,  256, "Batch size for dump");  //NOLINT
DEFINE_int32(write_batch_size, 256, "Batch size for load");  //NOLINT

static constexpr std::string_view KEY_LOCATION { "location" };  //NOLINT

int backend_main(int argc, char **argv) {
    // database environment
    auto env = jogasaki::api::create_environment();
    env->initialize();

    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // database
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->prepare_benchmark_tables(true);
    auto db = jogasaki::api::create_database(cfg);
    db->start();
    DBCloser dbcloser{db};

    // communication channel
    std::unique_ptr<ogawayama::common::SharedMemory> shared_memory;
    std::unique_ptr<ogawayama::common::ChannelStream> server_ch;
    try {
        shared_memory = std::make_unique<ogawayama::common::SharedMemory>(FLAGS_dbname, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_SERVER_CHANNEL, true, FLAGS_remove_shm);
        server_ch = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory.get(), true, false);
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << std::endl;
        return -1;
    }

    SignalHandler signal_handler{[&server_ch](){
            server_ch->lock();
            server_ch->send_req(ogawayama::common::CommandMessage::Type::TERMINATE);
            server_ch->wait();
            server_ch->unlock();
    }};

    std::vector<std::unique_ptr<Worker>> workers;
    int rv = 0;
    while(true) {
        ogawayama::common::CommandMessage::Type type;
        std::size_t index;
        std::string_view string;
        try {
            auto rv = server_ch->recv_req(type, index, string);
            if (rv != ERROR_CODE::OK) {
                if (rv != ERROR_CODE::TIMEOUT) {
                    std::cerr << __func__ << " " << __LINE__ <<  " " << ogawayama::stub::error_name(rv) << std::endl;
                }
                continue;
            }
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            rv = -1;
            goto finish;
        }

        switch (type) {
        case ogawayama::common::CommandMessage::Type::CONNECT:
            if (workers.size() < (index + 1)) {
                workers.resize(index + 1);
            }
            try {
                std::unique_ptr<Worker> &worker = workers.at(index);
                worker = std::make_unique<Worker>(*db, index);
                worker->task_ = std::packaged_task<void()>([&]{worker->run();});
                worker->future_ = worker->task_.get_future();
                worker->thread_ = std::thread(std::move(worker->task_));
            } catch (std::exception &ex) {
                std::cerr << ex.what() << std::endl;
                rv = -1; goto finish;
            }
            break;
        case ogawayama::common::CommandMessage::Type::DUMP_DATABASE:
            try {
                std::string name(string);
                dump(*db, FLAGS_location, name);
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
            break;
        case ogawayama::common::CommandMessage::Type::LOAD_DATABASE:
            try {
                std::string name(string);
                load(*db, FLAGS_location, name);
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
            break;
        case ogawayama::common::CommandMessage::Type::TERMINATE:
            server_ch->notify();
            server_ch->lock();
            server_ch->unlock();
            goto finish;
        default:
            std::cerr << "unsurpported message" << std::endl;
            rv = -1;
            server_ch->notify();
            goto finish;
        }
        server_ch->notify();
    }

 finish:
    signal_handler.shutdown();
    return rv;
}
    
}  // ogawayama::server
