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

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <unordered_map>
#include <jogasaki/api.h>
#include <ogawayama/stub/api.h>
#include "fe_provider.h"

#include "stubImpl.h"
#include "connectionImpl.h"
#include "transactionImpl.h"
#include "result_setImpl.h"

const char* const shm_name = "ogawayama_test";

namespace ogawayama::bridge {

struct ipc_endpoint_context {
    std::unordered_map<std::string, std::string> options_{};
};

class envelope {
public:
    envelope() {
        // environment
        auto env = std::make_shared<tateyama::api::environment>();
        std::string dir {""};  // FIXME have to update tateyama/configuration.h
        if (auto conf = tateyama::api::configuration::create_configuration(dir); conf != nullptr) {
            env->configuration(conf);
        } else {
            LOG(ERROR) << "error in create_configuration";
            exit(1);
        }

        // database
        auto cfg = std::make_shared<jogasaki::configuration>();
        cfg->thread_pool_size(1);  // FLAGS_threads

        db_ = jogasaki::api::create_database(cfg);
        db_->start();

        ipc_endpoint_context init_context;
        init_context.options_ = std::unordered_map<std::string, std::string>{
            {"dbname", shm_name},
            {"threads", std::to_string(1)},
        };

        bridge_ = std::move(ogawayama::bridge::fe_provider::create());
        bridge_->initialize(*env, db_.get(), std::addressof(init_context));
        bridge_->start();
    }
    ~envelope() {
        bridge_->shutdown();
        db_->stop();
    }
private:
    std::unique_ptr<jogasaki::api::database> db_;
    std::shared_ptr<fe_provider> bridge_;
};

}
