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
#pragma once

#include <future>
#include <thread>

#include <tateyama/status.h>
#include <tateyama/api/endpoint/request.h>
#include <tateyama/api/endpoint/response.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/endpoint/service.h>
#include <jogasaki/api.h>

#include "ipc_request.h"
#include "ipc_response.h"
#include "server_wires.h"

namespace ogawayama::server {

class Worker {
 public:
    Worker(tateyama::api::endpoint::service& service, std::size_t session_id, std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire)
        : service_(service), wire_(std::move(wire)), request_wire_container_(wire_->get_request_wire()), session_id_(session_id), garbage_collector_() {}
    ~Worker() {
        if(thread_.joinable()) thread_.join();
    }
    void run();
    friend int backend_main(int, char **);

 private:
    tateyama::api::endpoint::service& service_;
    std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire_;
    tsubakuro::common::wire::server_wire_container::wire_container& request_wire_container_;
    std::size_t session_id_;
    tsubakuro::common::wire::garbage_collector garbage_collector_;

    // for future
    std::packaged_task<void()> task_;
    std::future<void> future_;
    std::thread thread_{};
};

}  // ogawayama::server
