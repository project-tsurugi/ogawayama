/*
 * Copyright 2023-2023 Project Tsurugi.
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
#include <condition_variable>
#include <array>

#include <boost/archive/binary_oarchive.hpp>

#include <glog/logging.h>

#include <unordered_map>
#include <ogawayama/stub/api.h>
#include <tateyama/utils/protobuf_utils.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/response.pb.h>

#include "tateyama/framework/component_ids.h"
#include "server_wires_impl.h"
#include "endpoint_proto_utils.h"
#include "tateyama/authentication/crypto/rsa.h"
#include "tateyama/authentication/crypto/base64.h"
#include "tateyama/authentication/crypto/key.h"
#include "ogawayama/stub/stubImpl.h"
#include "ogawayama/stub/connectionImpl.h"
#include "ogawayama/stub/transactionImpl.h"
#include "ogawayama/stub/result_setImpl.h"

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

// The number of workers is limited to 1, as this is for testing purposes, 
class endpoint {
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_BODY = 1;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_BODYHEAD = 2;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_HANDSHAKE = 98;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_HELLO = 99;

    constexpr static std::size_t response_array_size = 5;

public:
    class worker {
      public:
        worker(std::string session_id, std::unique_ptr<tateyama::common::server_wire::server_wire_container> wire, std::function<void(void)> clean_up, endpoint* ep)
            : session_id_(session_id), wire_(std::move(wire)), clean_up_(std::move(clean_up)), endpoint_(ep) {
        }
        void operator()() {
            while(true) {
                auto h = wire_->get_request_wire()->peep();
                auto index = h.get_idx();
                if (h.get_length() == 0 && index == tateyama::common::wire::message_header::terminate_request) { break; }
                std::string message;
                message.resize(h.get_length());
                wire_->get_request_wire()->read(message.data());

                {
                    if (endpoint_->framework_error_) {
                        send_framework_error(index);
                        continue;
                    }
                    ::tateyama::proto::framework::request::Header req_header{};
                    google::protobuf::io::ArrayInputStream in{message.data(), static_cast<int>(message.length())};
                    if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(req_header), std::addressof(in), nullptr); ! res) {
                        throw std::runtime_error("error parsing request message");
                    }
                    std::stringstream ss{};
                    if (auto service_id = req_header.service_id(); service_id == tateyama::framework::service_id_endpoint_broker) {
                        // obtain request
                        std::string_view payload{};
                        if (auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), nullptr, payload); ! res) {
                            throw std::runtime_error("error reading payload");
                        }
                        tateyama::proto::endpoint::request::Request rq{};
                        if(!rq.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
                            throw std::runtime_error("request parse error");
                        }

                        // prepare for response (framework header)
                        ::tateyama::proto::framework::response::Header header{};
                        header.set_payload_type(tateyama::proto::framework::response::Header::SERVICE_RESULT);
                        if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss)); ! res) {
                            throw std::runtime_error("error formatting response message");
                        }

                        // EncryptionKey request
                        if (rq.command_case() == tateyama::proto::endpoint::request::Request::kEncryptionKey) {
                            tateyama::proto::endpoint::response::EncryptionKey rp{};
                            auto rs = rp.mutable_success();
                            rs->set_encryption_key(tateyama::authentication::crypto::base64_decode(tateyama::authentication::crypto::public_key));
                            auto body = rp.SerializeAsString();
                            if(auto res = tateyama::utils::PutDelimitedBodyToOstream(body, std::addressof(ss)); ! res) {
                                throw std::runtime_error("error formatting response message");
                            }
                            rp.clear_success();

                            auto reply_message = ss.str();
                            wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
                            continue;
                        }

                        // Handshake request
                        tateyama::proto::endpoint::response::Handshake rp{};
                        auto rs = rp.mutable_success();
                        rs->set_session_id(1);  // session id is dummy, as this is a test
                        auto body = rp.SerializeAsString();
                        if(auto res = tateyama::utils::PutDelimitedBodyToOstream(body, std::addressof(ss)); ! res) {
                            throw std::runtime_error("error formatting response message");
                        }
                        rp.clear_success();
                        auto reply_message = ss.str();
                        wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_HANDSHAKE));
                        continue;

                    } else if (service_id == tateyama::framework::service_id_routing) {
                        ::tateyama::proto::framework::response::Header header{};
                        header.set_payload_type(tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT);
                        if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss)); ! res) {
                            throw std::runtime_error("error formatting response message");
                        }
                        tateyama::proto::core::response::UpdateExpirationTime rp{};
                        (void) rp.mutable_success();
                        auto body = rp.SerializeAsString();
                        if(auto res = tateyama::utils::PutDelimitedBodyToOstream(body, std::addressof(ss)); ! res) {
                            throw std::runtime_error("error formatting response message");
                        }
                        rp.clear_success();
                        auto reply_message = ss.str();
                        wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_HANDSHAKE));
                        continue;

                    }
                    if (req_header.service_id() != tateyama::framework::service_id_sql) {
                        std::stringstream ess{};
                        ess << "unknown service_id: " << req_header.service_id();
                        throw std::runtime_error(ess.str());
                    }
                }

                // handle SQL
                endpoint_->requests_.push(message);
                auto reply = endpoint_->responses_.front();
                endpoint_->responses_.pop();
                if (reply.get_type() == endpoint_response::BODY_ONLY) {
                    auto reply_message = reply.get_body();
                    wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
                } else if (reply.get_type() == endpoint_response::WHTH_BODYHEAD) {
                    resultset_wires_array_.at(index) = wire_->create_resultset_wires(reply.get_name());
                    auto& resultset_wires = resultset_wires_array_.at(index);
                    resultset_wire_array_.at(index) = resultset_wires->acquire();
                    auto& resultset_wire = resultset_wire_array_.at(index);
                    // body_head
                    auto body_head = reply.get_body_head();
                    wire_->get_response_wire().write(body_head.data(), tateyama::common::wire::response_header(index, body_head.length(), RESPONSE_BODYHEAD));

                    // resultset
                    auto &resultset = reply.get_resultset();
                    while (!resultset.empty()) {
                        auto chunk = resultset.front();
                        resultset_wire->write(chunk.data(), chunk.length());
                        resultset_wire->flush();
                        resultset.pop();
                    }
                    resultset_wires->set_eor();

                    // body
                    auto reply_message = reply.get_body();
                    wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
                } else {
                    throw std::runtime_error("response for the request has not been set");
                }
            }
            clean_up_();
        }
    private:
        std::string session_id_;
        std::unique_ptr<tateyama::common::server_wire::server_wire_container> wire_;
        std::function<void(void)> clean_up_;
        endpoint* endpoint_;

        std::array<tateyama::common::server_wire::server_wire_container::unq_p_resultset_wires_conteiner, response_array_size> resultset_wires_array_;
        std::array<tateyama::common::server_wire::server_wire_container::unq_p_resultset_wire_conteiner, response_array_size> resultset_wire_array_;

        void send_framework_error(int index) {
            std::stringstream ss{};
            if(auto res = tateyama::utils::SerializeDelimitedToOstream(endpoint_->framework_header_, std::addressof(ss)); ! res) {
                throw std::runtime_error("error formatting response message");
            }
            ::tateyama::proto::diagnostics::Record r{};
            r.set_code(::tateyama::proto::diagnostics::Code::OPERATION_CANCELED);
            auto record = r.SerializeAsString();
            if(auto res = tateyama::utils::PutDelimitedBodyToOstream(record, std::addressof(ss)); ! res) {
                throw std::runtime_error("error formatting record message");
            }
            auto reply_message = ss.str();
            wire_->get_response_wire().write(reply_message.data(), tateyama::common::wire::response_header(index, reply_message.length(), RESPONSE_BODY));
        }
    };

    endpoint(std::string name)
        : name_(name), container_(std::make_unique<tateyama::common::server_wire::connection_container>(name_, 1)) {
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
            std::size_t index = connection_queue.slot();
            connection_queue.accept(index, session_id);
            try {
                worker_ = std::make_unique<worker>(session_name, std::move(wire), [&connection_queue, index](){ connection_queue.disconnect(index); }, this);
                thread_ = std::thread(std::ref(*worker_));
            } catch (std::exception& ex) {
                LOG(ERROR) << ex.what();
                break;
            }
        }
    }
    void terminate() {
        container_->get_connection_queue().request_terminate();
    }

    void response_message(const jogasaki::proto::sql::response::Response& message) {
        std::stringstream ss{};
        ::tateyama::proto::framework::response::Header header{};
        header.set_payload_type(tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT);
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(header, std::addressof(ss)); ! res) {
            throw std::runtime_error("error formatting response message");
        }
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(message, std::addressof(ss)); ! res) {
            throw std::runtime_error("error formatting response message");
        }
        responses_.emplace(endpoint_response(ss.str()));
    }
    void response_message(const jogasaki::proto::sql::response::Response& head, std::string_view name, std::queue<std::string>& resultset, const jogasaki::proto::sql::response::Response& body) {
        std::stringstream ss_head{};
        ::tateyama::proto::framework::response::Header header{};
        header.set_payload_type(tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT);
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
        responses_.emplace(endpoint_response(ss_head.str(), name, resultset, ss_body.str()));
    }
    std::string_view request_message() {
        current_request_ = requests_.front();
        requests_.pop();
        return current_request_;
    }
    void framework_error(::tateyama::proto::framework::response::Header& h) {
        framework_error_ = true;
        framework_header_ = h;
    }


private:
    std::string name_;
    std::unique_ptr<tateyama::common::server_wire::connection_container> container_;
    std::thread thread_;
    std::unique_ptr<worker> worker_{};

    std::queue<std::string> requests_{};
    std::queue<endpoint_response> responses_{};
    std::string current_request_{};
    bool framework_error_{};
    ::tateyama::proto::framework::response::Header framework_header_{};
};

}  // namespace ogawayama::testing
