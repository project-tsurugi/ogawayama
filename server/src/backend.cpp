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
#include <chrono>
#include <csignal>
#include <setjmp.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>

#include "worker.h"
#include "SignalHandler.h"
#include "utils.h"

#include "server.h"

namespace ogawayama::server {

DEFINE_string(dbname, "tsubakuro", "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT
DEFINE_int32(read_batch_size,  256, "Batch size for dump");  //NOLINT
DEFINE_int32(write_batch_size, 256, "Batch size for load");  //NOLINT

static constexpr std::string_view KEY_LOCATION { "location" };  //NOLINT


jmp_buf buf;

void signal_handler(int signal)
{
    longjmp(buf, 1);
}

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
    auto container = std::make_unique<tsubakuro::common::wire::connection_container>(FLAGS_dbname);

    // worker objects
    std::vector<std::unique_ptr<Worker>> workers;

    // singal handler
    std::signal(SIGINT, signal_handler);
    if (setjmp(buf) != 0) {
        for (std::size_t index = 0; index < workers.size() ; index++) {
            if (auto rv = workers.at(index)->future_.wait_for(std::chrono::seconds(0)) ; rv != std::future_status::ready) {
                VLOG(1) << "exit: remain thread " << workers.at(index)->id_ << std::endl;
            }
        }
        workers.clear();
        container = nullptr;;
        return 0;
    }

    int return_value{0};
    while(true) {
        auto session_id = container->get_connection_queue().listen(true);
        VLOG(1) << "connect request: " << session_id << std::endl;
        std::string session_name = FLAGS_dbname;
        session_name += "-";
        session_name += std::to_string(session_id);
        auto wire = std::make_unique<tsubakuro::common::wire::server_wire_container>(session_name);
        container->get_connection_queue().accept(session_id);
        std::size_t index;
        for (index = 0; index < workers.size() ; index++) {
            if (auto rv = workers.at(index)->future_.wait_for(std::chrono::seconds(0)) ; rv == std::future_status::ready) {
                break;
            }
        }
        if (workers.size() < (index + 1)) {
            workers.resize(index + 1);
        }
        try {
            std::unique_ptr<Worker> &worker = workers.at(index);
            worker = std::make_unique<Worker>(*db, session_id, std::move(wire));
            worker->task_ = std::packaged_task<void()>([&]{worker->run();});
            worker->future_ = worker->task_.get_future();
            worker->thread_ = std::thread(std::move(worker->task_));
        } catch (std::exception &ex) {
            std::cerr << ex.what() << std::endl;
            return_value = -1; goto finish;
        }
    }

  finish:
    return return_value;
}

}  // ogawayama::server
