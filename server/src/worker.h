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

#include "jogasaki/api.h"
#include "manager/metadata/tables.h"
#include "manager/metadata/metadata.h"
#include "manager/metadata/error_code.h"
#include "manager/metadata/datatypes.h"

#include "server_wires.h"
#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"

namespace ogawayama::server {

class Worker {
    class Cursor {
    public:
        void clear() {
            result_set_ = nullptr;
            iterator_ = nullptr;
            prepared_ = nullptr;
        }
        std::unique_ptr<jogasaki::api::result_set> result_set_{};
        std::unique_ptr<jogasaki::api::result_set_iterator> iterator_{};
        std::unique_ptr<jogasaki::api::prepared_statement> prepared_{};
        std::unique_ptr<tsubakuro::common::wire::server_wire_container::resultset_wire_container> resultset_wire_container_{};
        std::string wire_name_;
    };

 public:
    Worker(jogasaki::api::database& db, std::size_t id, std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire) :
        db_(db), id_(id), wire_(std::move(wire)), request_wire_container_(wire_->get_request_wire()) {}
    ~Worker() {
        clear_all();
        if(thread_.joinable()) thread_.join();
    }
    void run();
    friend int backend_main(int, char **);

 private:
    [[nodiscard]] const char* execute_statement(std::string_view);
    [[nodiscard]] const char* execute_query(std::string_view, std::size_t);
    void next(std::size_t);
    [[nodiscard]] const char* execute_prepared_statement(std::size_t, jogasaki::api::parameter_set&);
    [[nodiscard]] const char* execute_prepared_query(std::size_t, jogasaki::api::parameter_set&, std::size_t);
    void deploy_metadata(std::size_t);

    void send_metadata(std::size_t);
    void set_params(request::ParameterSet*, std::unique_ptr<jogasaki::api::parameter_set>&);
    void clear_transaction() {
        cursors_.clear();
        transaction_ = nullptr;
    }
    void clear_all() {
        clear_transaction();
        prepared_statements_.clear();
        wire_ = nullptr;
    }
    void reply(protocol::Response &r, tsubakuro::common::wire::message_header::index_type idx) {
        tsubakuro::common::wire::response_wrapper buf(wire_->get_response(idx));
        std::ostream os(&buf);
        if (!r.SerializeToOstream(&os)) { std::abort(); }
        buf.flush();
    }

    template<typename T>
    void error(std::string msg, tsubakuro::common::wire::message_header::index_type idx) {}
        
    jogasaki::api::database& db_;
    std::size_t id_;

    std::unique_ptr<jogasaki::api::transaction> transaction_;
    std::size_t transaction_id_{};
    std::size_t resultset_id_{};
    std::vector<Cursor> cursors_;
    std::vector<std::unique_ptr<jogasaki::api::prepared_statement>> prepared_statements_{};
    std::size_t prepared_statements_index_{};

    std::packaged_task<void()> task_;
    std::future<void> future_;
    std::thread thread_{};

    std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire_;
    tsubakuro::common::wire::server_wire_container::wire_container& request_wire_container_;

    std::string dummy_string_;
    std::size_t search_resultset() {
        for (std::size_t i = 0; i < cursors_.size() ; i++) {
            auto* wire_container = cursors_.at(i).resultset_wire_container_.get();
            if (wire_container->is_closed()) {
                wire_container->initialize();
                return i;
            }
        }
        return cursors_.size();
    }
};

template<>
inline void Worker::error<protocol::Begin>(std::string msg, tsubakuro::common::wire::message_header::index_type idx) {
    protocol::Error e;
    protocol::Begin p;
    protocol::Response r;

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_begin(&p);
    reply(r, idx);
    r.release_begin();
    p.release_error();
}
template<>
inline void Worker::error<protocol::Prepare>(std::string msg, tsubakuro::common::wire::message_header::index_type idx) {
    protocol::Error e;
    protocol::Prepare p;
    protocol::Response r;

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_prepare(&p);
    reply(r, idx);
    r.release_prepare();
    p.release_error();
}
template<>
inline void Worker::error<protocol::ResultOnly>(std::string msg, tsubakuro::common::wire::message_header::index_type idx) {
    protocol::Error e;
    protocol::ResultOnly p;
    protocol::Response r;

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_result_only(&p);
    reply(r, idx);
    r.release_result_only();
    p.release_error();
}
template<>
inline void Worker::error<protocol::ExecuteQuery>(std::string msg, tsubakuro::common::wire::message_header::index_type idx) {
    protocol::Error e;
    protocol::ExecuteQuery p;
    protocol::Response r;

    e.set_detail(msg);
    p.set_allocated_error(&e);
    r.set_allocated_execute_query(&p);
    reply(r, idx);
    r.release_execute_query();
    p.release_error();
}

}  // ogawayama::server
