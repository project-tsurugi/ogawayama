/*
 * Copyright 2023-2023 tsurugi project.
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

#include <string>
#include <thread>
#include <stdexcept>
#include <functional>
#include <mutex>
#include <array>

#include <glog/logging.h>

#include <unordered_map>
#include <jogasaki/api.h>
#include <ogawayama/stub/api.h>
#include <ogawayama/bridge/service.h>
#include <tateyama/framework/server.h>
#include <tateyama/utils/protobuf_utils.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/response.pb.h>

#include "server_wires_impl.h"
#include "endpoint_proto_utils.h"
#include "stubImpl.h"
#include "connectionImpl.h"
#include "transactionImpl.h"
#include "result_setImpl.h"

namespace ogawayama::testing {

class endpoint_response {
public:
    enum type {
        UNDEFINED = 0,
        BODY_ONLY = 1,
        WHTH_BODYHEAD = 2
    };
    endpoint_response() {
        type_ = UNDEFINED;
    }
    explicit endpoint_response(std::string body) {
        body_ = body;
        type_ = BODY_ONLY;
    }
    endpoint_response(std::string body_head, std::string_view name, std::queue<std::string>& resultset, std::string body) {
        body_head_ = body_head;
        name_ = name;
        resultset_ = &resultset;
        body_ = body;
        type_ = WHTH_BODYHEAD;
    }
    std::string_view get_body() const {
        return body_;
    }
    std::string_view get_body_head() const {
        return body_head_;
    }
    std::queue<std::string>& get_resultset() const {
        return *resultset_;
    }
    std::string_view get_name() const {
        return name_;
    }
    type get_type() const {
        return type_;
    }
private:
    std::string body_{};
    std::string body_head_{};
    std::string name_{};
    std::queue<std::string>* resultset_{};
    type type_{};
};

class endpoint {
    friend class server;

    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_BODY = 1;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_BODYHEAD = 2;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_CODE = 3;

    constexpr static std::size_t response_array_size = 5;

public:
    class worker {
      public:
        worker(std::string session_id, std::unique_ptr<tateyama::common::server_wire::server_wire_container> wire, std::function<void(void)> clean_up)
            : session_id_(session_id), wire_(std::move(wire)), clean_up_(std::move(clean_up)), thread_(std::thread(std::ref(*this))) {
        }
        ~worker() {
            if (thread_.joinable()) {
                thread_.join();
            }
        }
        void operator()() {
            while(true) {
                auto h = wire_->get_request_wire()->peep(true);
                auto index = h.get_idx();
                if (h.get_length() == 0 && index == tateyama::common::wire::message_header::not_use) { break; }
                std::string message;
                message.resize(h.get_length());
                wire_->get_request_wire()->read(message.data());
                requests_.push(message);

                auto& reply = responses_.at(index);
                if (reply.get_type() == endpoint_response::BODY_ONLY) {
                    auto reply_message = reply.get_body();
                    wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
                } else if (reply.get_type() == endpoint_response::WHTH_BODYHEAD) {
                    resultset_wires_array_.at(index) = wire_->create_resultset_wires(reply.get_name());
                    auto* resultset_wires_ = (resultset_wires_array_.at(index)).get();
                    resultset_wire_ = resultset_wires_->acquire();
                    // body_head
                    auto body_head = reply.get_body_head();
                    wire_->get_response_wire().write(body_head.data(), tateyama::common::wire::response_header(index, body_head.length(), RESPONSE_BODYHEAD));

                    // resultset
                    auto &resultset = reply.get_resultset();
                    while (!resultset.empty()) {
                        auto chunk = resultset.front();
                        resultset_wire_->write(chunk.data(), chunk.length());
                        resultset_wire_->flush();
                        resultset.pop();
                    }
                    resultset_wires_->set_eor();

                    // body
                    auto reply_message = reply.get_body();
                    wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
                } else {
                    throw std::runtime_error("response for the request has not been set");
                }
                reply = endpoint_response();
            }
            clean_up_();
        }
        void response_message(const jogasaki::proto::sql::response::Response& message) {
            std::stringstream ss{};
            ::tateyama::proto::framework::response::Header header{};
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(message, std::addressof(ss)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            responses_.at(0) = endpoint_response(ss.str());
        }
        void response_message(const jogasaki::proto::sql::response::Response& head, std::string_view name, std::queue<std::string>& resultset, const jogasaki::proto::sql::response::Response& body, std::size_t index) {
            std::stringstream ss_head{};
            ::tateyama::proto::framework::response::Header header{};
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss_head)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(head, std::addressof(ss_head)); ! res) {
                throw std::runtime_error("error formatting response message");
            }

            std::stringstream ss_body{};
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss_body)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(body, std::addressof(ss_body)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            responses_.at(index) = endpoint_response(ss_head.str(), name, resultset, ss_body.str());
        }
        std::string_view request_message() {
            current_request_ = requests_.front();
            requests_.pop();
            return current_request_;
        }
    private:
        std::string session_id_;
        std::unique_ptr<tateyama::common::server_wire::server_wire_container> wire_;
        std::function<void(void)> clean_up_;
        std::thread thread_;

        std::queue<std::string> requests_;
        std::array<endpoint_response, response_array_size> responses_;
        std::array<tateyama::common::server_wire::server_wire_container::unq_p_resultset_wires_conteiner, response_array_size> resultset_wires_array_;
        std::string current_request_;

        tateyama::common::server_wire::server_wire_container::unq_p_resultset_wires_conteiner resultset_wires_{};
        tateyama::common::server_wire::server_wire_container::unq_p_resultset_wire_conteiner resultset_wire_{};
    };

    endpoint(std::string name)
        : name_(name), container_(std::make_unique<tateyama::common::server_wire::connection_container>(name_, 1)), thread_(std::thread(std::ref(*this))) {
    }
    ~endpoint() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    void operator()() {
        auto& connection_queue = container_->get_connection_queue();

        while(true) {
            auto session_id = connection_queue.listen();
            if (connection_queue.is_terminated()) {
                connection_queue.confirm_terminated();
                break;
            }
            std::string session_name = name_;
            session_name += "-";
            session_name += std::to_string(session_id);
            auto wire = std::make_unique<tateyama::common::server_wire::server_wire_container>(session_name);
            std::size_t index = connection_queue.accept(session_id);
            try {
                worker_ = std::make_unique<worker>(session_name, std::move(wire), [&connection_queue, index](){ connection_queue.disconnect(index); });
            } catch (std::exception& ex) {
                LOG(ERROR) << ex.what();
                break;
            }
        }
    }
    void terminate() {
        container_->get_connection_queue().request_terminate();
    }

private:
    std::string name_;
    std::unique_ptr<tateyama::common::server_wire::connection_container> container_;
    std::thread thread_;
    std::unique_ptr<worker> worker_{};
};

}  // namespace ogawayama::testing
