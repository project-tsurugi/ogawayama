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
#include <ogawayama/bridge/service.h>
#include <tateyama/framework/server.h>

#include "stubImpl.h"
#include "connectionImpl.h"
#include "transactionImpl.h"
#include "result_setImpl.h"

const char* const shm_name = "ogawayama_test";

namespace ogawayama::bridge {

class envelope {
public:
    envelope() {
        // environment
        if (auto conf = tateyama::api::configuration::create_configuration("../../../stub/test/configuration/tsurugi.ini"); conf != nullptr) {
            sv_ = std::make_shared<tateyama::framework::server>(tateyama::framework::boot_mode::database_server, conf);
        } else {
            LOG(ERROR) << "error in create_configuration";
            exit(1);
        }
        add_core_components(*sv_);
        auto sqlres = std::make_shared<jogasaki::api::resource::bridge>();
        sv_->add_resource(sqlres);
        sv_->add_service(std::make_shared<ogawayama::bridge::service>());
        sv_->setup();
        auto& cfg = sqlres->database()->config();
        cfg->thread_pool_size(1);  // FLAGS_threads
        sv_->start();
    }
    ~envelope() {
        sv_->shutdown();
    }
private:
    std::shared_ptr<tateyama::framework::server> sv_;
};

}
