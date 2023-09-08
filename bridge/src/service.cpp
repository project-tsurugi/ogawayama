/*
 * Copyright 2019-2021 tsurugi project.
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

DECLARE_bool(remove_shm);

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

bool service::operator()(std::shared_ptr<tateyama::api::server::request> req, std::shared_ptr<tateyama::api::server::response> res) {
    auto payload = req->payload();
    std::istringstream ifs(std::string{payload});
    boost::archive::binary_iarchive ia(ifs);

    ogawayama::common::command c{};
    ia >> c;
    VLOG(log_trace) << "received " << ogawayama::common::to_string_view(c);

    ERROR_CODE rv{ERROR_CODE::OK};
    switch(c) {
    case ogawayama::common::command::create_table:
        rv = Worker::do_deploy_table(*db_, ia);
        break;
    case ogawayama::common::command::drop_table:
        rv = Worker::do_withdraw_table(*db_, ia);
        break;
    case ogawayama::common::command::create_index:
        rv = Worker::do_deploy_index(*db_, ia);
        break;
    case ogawayama::common::command::drop_index:
        rv = Worker::do_withdraw_index(*db_, ia);
        break;
    case ogawayama::common::command::begin:
        rv = Worker::begin_ddl(*db_);
        break;
    case ogawayama::common::command::commit:
        rv = Worker::end_ddl(*db_);
        break;
    default:
        return false;
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
