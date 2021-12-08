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
#include <signal.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>

#include "fe_provider.h"
#include "server.h"

namespace ogawayama::server {

DEFINE_string(dbname, "ogawayama", "database name");  // NOLINT
DEFINE_string(location, "./db", "database location on file system");  // NOLINT
DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT
DEFINE_int32(read_batch_size,  256, "Batch size for dump");  //NOLINT
DEFINE_int32(write_batch_size, 256, "Batch size for load");  //NOLINT

static constexpr std::string_view KEY_LOCATION { "location" };  //NOLINT

struct ipc_endpoint_context {
    std::unordered_map<std::string, std::string> options_{};
};

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

    ipc_endpoint_context init_context{};
    init_context.options_ = std::unordered_map<std::string, std::string>{
        {"dbname", FLAGS_dbname},
        {"threads", std::to_string(128)},
    };

    auto bridge = std::move(ogawayama::bridge::fe_provider::create());
    bridge->initialize(db.get(), std::addressof(init_context));
    bridge->start();
        
    // wait for signal to terminate this
    int signo;
    sigset_t ss;
    sigemptyset(&ss);
    do {
        if (auto ret = sigaddset(&ss, SIGINT); ret != 0) {
            LOG(ERROR) << "fail to sigaddset";
        }
        if (auto ret = sigprocmask(SIG_BLOCK, &ss, NULL); ret != 0) {
            LOG(ERROR) << "fail to pthread_sigmask";
        }
        if (auto ret = sigwait(&ss, &signo); ret == 0) { // シグナルを待つ
            switch(signo) {
            case SIGINT:
                // termination process
                if (bridge) {
                    LOG(INFO) << "bridge->shutdown()";
                    bridge->shutdown();
                }
                return 0;
            }
        } else {
            LOG(ERROR) << "fail to sigwait";
            return -1;
        }
    } while(true);
}
    
}  // ogawayama::server
