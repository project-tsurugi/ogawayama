/*
 * Copyright 2019-2023 Project Tsurugi.
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
#include <ogawayama/bridge/service.h>

#include <memory>
#include <string>
#include <exception>
#include <sstream>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <tateyama/framework/environment.h>

#include <ogawayama/common/bridge.h>
#include <ogawayama/logging.h>
#include "worker.h"

namespace ogawayama::bridge {

service::service() = default;

service::~service() {
    VLOG(log_info) << "/:tateyama:lifecycle:component:<dtor> " << component_label;
}

bool service::start(tateyama::framework::environment& env) {
    auto sqlres = env.resource_repository().find<jogasaki::api::resource::bridge>();
    if(! sqlres) {
        std::abort();
    }
    db_ = sqlres->database();
    return true;
}

bool service::shutdown(tateyama::framework::environment& env) {
    return true;
}

bool service::operator()(std::shared_ptr<tateyama::api::server::request> req, std::shared_ptr<tateyama::api::server::response> res) { //NOLINT(readability-function-cognitive-complexity)
    static thread_local std::unique_ptr<Worker> worker_for_this_thread{};

    auto payload = req->payload();
    std::istringstream ifs(std::string{payload});
    boost::archive::binary_iarchive ia(ifs);
    ERROR_CODE rv{ERROR_CODE::OK};

    std::uint32_t message_version{};
    ogawayama::common::command c{};
    ia >> message_version;
    if (message_version > common::OGAWAYAMA_MESSAGE_VERSION) {
        rv = ERROR_CODE::UNSUPPORTED;
    } else {
        ia >> c;
        VLOG(log_debug) << "--> " << ogawayama::common::to_string_view(c);

        switch(c) {
        case ogawayama::common::command::hello:
            rv = ERROR_CODE::OK;
            break;
        case ogawayama::common::command::create_table:
            if (worker_for_this_thread) {
                rv = worker_for_this_thread->do_deploy_table(*db_, ia);
            } else {
                rv = ERROR_CODE::NO_TRANSACTION;
            }
            break;
        case ogawayama::common::command::drop_table:
            if (worker_for_this_thread) {
                rv = worker_for_this_thread->do_withdraw_table(*db_, ia);
            } else {
                rv = ERROR_CODE::NO_TRANSACTION;
            }
            break;
        case ogawayama::common::command::create_index:
            if (worker_for_this_thread) {
                rv = worker_for_this_thread->do_deploy_index(*db_, ia);
            } else {
                rv = ERROR_CODE::NO_TRANSACTION;
            }
            break;
        case ogawayama::common::command::drop_index:
            if (worker_for_this_thread) {
                rv = worker_for_this_thread->do_withdraw_index(*db_, ia);
            } else {
                rv = ERROR_CODE::NO_TRANSACTION;
            }
            break;
        case ogawayama::common::command::begin:
            if (!worker_for_this_thread) {
                worker_for_this_thread = std::make_unique<Worker>();
                rv = worker_for_this_thread->begin_ddl(*db_);
            } else {
                rv = ERROR_CODE::TRANSACTION_ALREADY_STARTED;
            }
            break;
        case ogawayama::common::command::commit:
            if (worker_for_this_thread) {
                rv = worker_for_this_thread->end_ddl(*db_);
                worker_for_this_thread = nullptr;
            } else {
                rv = ERROR_CODE::NO_TRANSACTION;
            }
            break;
        default:
            return false;
        }
    }

    std::ostringstream ofs;
    boost::archive::binary_oarchive oa(ofs);
    oa << rv;
    res->body(ofs.str());
    return true;
}

tateyama::framework::component::id_type service::id() const noexcept {
    return tag;
}

bool service::setup(tateyama::framework::environment&) {
    return true;
}

std::string_view service::label() const noexcept {
    return component_label;
}

}  // ogawayama::bridge
