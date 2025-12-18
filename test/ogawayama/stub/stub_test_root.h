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

#include <stdexcept>
#include <functional>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <unordered_map>
#include <ogawayama/stub/api.h>
#include <tateyama/utils/protobuf_utils.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/response.pb.h>
#include <tateyama/proto/diagnostics.pb.h>
#include <tateyama/proto/framework/response.pb.h>

#include "server_wires_impl.h"
#include "endpoint_proto_utils.h"
#include "ogawayama/stub/stubImpl.h"
#include "ogawayama/stub/connectionImpl.h"
#include "ogawayama/stub/transactionImpl.h"
#include "ogawayama/stub/result_setImpl.h"
#include "endpoint.h"

namespace ogawayama::testing {

class server {
    constexpr static std::string_view resultset_name_prefix = "resultset_for_test_";  // NOLINT

public:
    server(std::string name) : name_(name), endpoint_(name_), thread_(std::thread(std::ref(endpoint_))) {
    }
    ~server() {
        endpoint_.terminate();
        if (thread_.joinable()) {
            thread_.join();
        }
        remove_shm();
    }

    template<typename T>
    void response_message(T& reply) = delete;
    template<typename T>
    void response_message(T& reply, std::size_t) = delete;

    void response_with_resultset(jogasaki::proto::sql::response::ResultSetMetadata& metadata, std::queue<std::string>& resultset, jogasaki::proto::sql::response::ResultOnly& ro) {
        std::string resultset_name{resultset_name_prefix};
        resultset_name += std::to_string(resultset_number_++);
        // body_head
        jogasaki::proto::sql::response::ExecuteQuery e{};
        jogasaki::proto::sql::response::Response rh{};
        e.set_name(std::string(resultset_name));
        e.set_allocated_record_meta(&metadata);
        rh.set_allocated_execute_query(&e);
        // body
        jogasaki::proto::sql::response::Response rb{};
        rb.set_allocated_result_only(&ro);
        // set response
        endpoint_.response_message(rh, resultset_name, resultset, rb);
        // release
        (void) rb.release_result_only();
        (void) rh.release_execute_query();
        (void) e.release_record_meta();
    }

    void response_body_head(jogasaki::proto::sql::response::ResultSetMetadata& metadata) {
        std::string resultset_name{resultset_name_prefix};
        resultset_name += std::to_string(resultset_number_++);
        // body_head
        jogasaki::proto::sql::response::ExecuteQuery e{};
        jogasaki::proto::sql::response::Response rh{};
        e.set_name(std::string(resultset_name));
        e.set_allocated_record_meta(&metadata);
        rh.set_allocated_execute_query(&e);

        endpoint_.response_body_head(rh, resultset_name);
        // release
        (void) rh.release_execute_query();
        (void) e.release_record_meta();
    }

    std::optional<jogasaki::proto::sql::request::Request> request_message() {
        auto request_packet = endpoint_.request_message();

        ::tateyama::proto::framework::request::Header header{};
        google::protobuf::io::ArrayInputStream in{request_packet.data(), static_cast<int>(request_packet.length())};
        if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(header), std::addressof(in), nullptr); ! res) {
            return std::nullopt;
        }
        std::string_view payload{};
        if (auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), nullptr, payload); ! res) {
            return std::nullopt;
        }
        jogasaki::proto::sql::request::Request request{};
        if(auto res = request.ParseFromArray(payload.data(), payload.length()); ! res) {
            return std::nullopt;
        }
        return request;
    }

    bool is_response_empty() {
        return endpoint_.is_response_empty();
    }

private:
    std::string name_;
    endpoint endpoint_;
    std::size_t resultset_number_{};
    std::thread thread_;

    void remove_shm() {
        std::string cmd = "if [ -f /dev/shm/" + name_ + " ]; then rm -f /dev/shm/" + name_ + "*; fi";
        if (system(cmd.c_str())) {
            throw std::runtime_error("error in clearing shared memory file");
        }
    }
};

template<>
inline void server::response_message<jogasaki::proto::sql::response::Begin>(jogasaki::proto::sql::response::Begin& b) {
    jogasaki::proto::sql::response::Response r{};
    r.set_allocated_begin(&b);
    endpoint_.response_message(r);
    (void) r.release_begin();
}
template<>
inline void server::response_message<jogasaki::proto::sql::response::ResultOnly>(jogasaki::proto::sql::response::ResultOnly& ro) {
    jogasaki::proto::sql::response::Response r{};
    r.set_allocated_result_only(&ro);
    endpoint_.response_message(r);
    (void) r.release_result_only();
}
template<>
inline void server::response_message<jogasaki::proto::sql::response::Prepare>(jogasaki::proto::sql::response::Prepare& p) {
    jogasaki::proto::sql::response::Response r{};
    r.set_allocated_prepare(&p);
    endpoint_.response_message(r);
    (void) r.release_prepare();
}
template<>
inline void server::response_message<jogasaki::proto::sql::response::ExecuteResult>(jogasaki::proto::sql::response::ExecuteResult& er) {
    jogasaki::proto::sql::response::Response r{};
    r.set_allocated_execute_result(&er);
    endpoint_.response_message(r);
    (void) r.release_execute_result();
}
template<>
inline void server::response_message<tateyama::proto::diagnostics::Code>(tateyama::proto::diagnostics::Code& code) {
    endpoint_.response_message(code);
}

}  // namespace ogawayama::testing
