/*
 * Copyright 2022-2023 Project Tsurugi.
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

#include <sstream>
#include <optional>
#include <string>
#include <vector>

#include <tateyama/utils/protobuf_utils.h>
#include <tateyama/proto/framework/request.pb.h>
#include <tateyama/proto/framework/response.pb.h>

#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/response.pb.h>

#include <ogawayama/stub/error_code.h>

#include "tateyama/transport/client_wire.h"

namespace tateyama::bootstrap::wire {

constexpr static tateyama::common::wire::message_header::index_type SLOT_FOR_DISPOSE_TRANSACTION = 1;
constexpr static tateyama::common::wire::message_header::index_type OPT_INDEX = SLOT_FOR_DISPOSE_TRANSACTION + 1;

class transport {
constexpr static std::size_t HEADER_MESSAGE_VERSION_MAJOR = 0;
constexpr static std::size_t HEADER_MESSAGE_VERSION_MINOR = 0;
constexpr static std::size_t SQL_MESSAGE_VERSION_MAJOR = 1;
constexpr static std::size_t SQL_MESSAGE_VERSION_MINOR = 0;
constexpr static std::uint32_t SERVICE_ID_SQL = 3;  // from tateyama/framework/component_ids.h
constexpr static std::uint32_t SERVICE_ID_FDW = 4;  // from tateyama/framework/component_ids.h

public:
    transport() = delete;

    explicit transport(tateyama::common::wire::session_wire_container& wire) : wire_(wire) {
        header_.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        header_.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        header_.set_service_id(SERVICE_ID_SQL);
        bridge_header_.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        bridge_header_.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        bridge_header_.set_service_id(SERVICE_ID_FDW);
    }

/**
 * @brief send a begin request to the sql service.
 * @param begin_request a begen request message in ::jogasaki::proto::sql::request::Begin
 * @return std::optional of ::jogasaki::proto::sql::request::Begin
 */
    std::optional<::jogasaki::proto::sql::response::Begin> send(::jogasaki::proto::sql::request::Begin& begin_request) {
        auto request = sql_request();
        *(request.mutable_begin())= begin_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_begin();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_begin()) {
                return response_message.begin();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a prepare request to the sql service.
 * @param prepare_request a begen request message in ::jogasaki::proto::sql::request::Prepare
 * @return std::optional of ::jogasaki::proto::sql::request::Prepare
 */
    std::optional<::jogasaki::proto::sql::response::Prepare> send(::jogasaki::proto::sql::request::Prepare& prepare_request) {
        auto request = sql_request();
        *(request.mutable_prepare())= prepare_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_prepare();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_prepare()) {
                return response_message.prepare();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute statement request to the sql service.
 * @param execute_statement_request a execute statement request message in ::jogasaki::proto::sql::request::ExecuteStatement
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteResult> send(::jogasaki::proto::sql::request::ExecuteStatement& execute_statement_request) {
        auto request = sql_request();
        *(request.mutable_execute_statement()) = execute_statement_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_execute_statement();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_result()) {
                return response_message.execute_result();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute statement request to the sql service.
 * @param execute_statement_request a execute statement request message in ::jogasaki::proto::sql::request::ExecutePreparedStatement
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteResult> send(::jogasaki::proto::sql::request::ExecutePreparedStatement& execute_statement_request) {
        auto request = sql_request();
        *(request.mutable_execute_prepared_statement()) = execute_statement_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_execute_statement();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_result()) {
                return response_message.execute_result();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute query request to the sql service.
 * @param execute_query_request a execute query request message in ::jogasaki::proto::sql::request::ExecuteQuery
 * @return std::optional of ::jogasaki::proto::sql::request::ExecuteQuery
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteQuery> send(::jogasaki::proto::sql::request::ExecuteQuery& execute_query_request, std::size_t query_index) {
        auto request = sql_request();
        *(request.mutable_execute_query()) = execute_query_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, query_index);
        request.clear_execute_query();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_query()) {
                return response_message.execute_query();
            }
            if (response_message.has_result_only()) {
                throw std::runtime_error("no body_head message");
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute query request to the sql service.
 * @param execute_query_request a execute query request message in ::jogasaki::proto::sql::request::ExecutePreparedQuery
 * @return std::optional of ::jogasaki::proto::sql::request::ExecutePreparedQuery
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteQuery> send(::jogasaki::proto::sql::request::ExecutePreparedQuery& execute_query_request, std::size_t query_index) {
        auto request = sql_request();
        *(request.mutable_execute_prepared_query()) = execute_query_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, query_index);
        request.clear_execute_prepared_query();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_query()) {
                return response_message.execute_query();
            }
            if (response_message.has_result_only()) {
                throw std::runtime_error("no body_head message");
            }
        }
        return std::nullopt;
    }

/**
 * @brief receive the receive_body describing the status of the execute_query processing.
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ResultOnly> receive_body(std::size_t query_index) {
        return receive<::jogasaki::proto::sql::response::ResultOnly>(query_index);
    }
    
/**
 * @brief send a commit request to the sql service.
 * @param commit_request a commit request message in ::jogasaki::proto::sql::request::Commit
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ResultOnly> send(::jogasaki::proto::sql::request::Commit& commit_request) {
        auto request = sql_request();
        *(request.mutable_commit()) = commit_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_commit();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                return response_message.result_only();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a rollback request to the sql service.
 * @param rollback_request a rollback request message in ::jogasaki::proto::sql::request::Rollback
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ResultOnly> send(::jogasaki::proto::sql::request::Rollback& rollback_request) {
        auto request = sql_request();
        *(request.mutable_rollback()) = rollback_request;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request);
        request.clear_rollback();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                return response_message.result_only();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a rollback request to the sql service.
 * @param rollback_request a rollback request message in ::jogasaki::proto::sql::request::Rollback
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<ogawayama::stub::ErrorCode> send(::jogasaki::proto::sql::request::DisposeTransaction& r) {
        auto request = sql_request();
        *(request.mutable_dispose_transaction()) = r;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, SLOT_FOR_DISPOSE_TRANSACTION);
        request.clear_rollback();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                auto ro = response_message.result_only();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
            if (response_message.has_dispose_transaction()) {
                auto ro = response_message.dispose_transaction();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
        }
        return std::nullopt;
    }

    void close() {
        wire_.close();
    }

    std::unique_ptr<tateyama::common::wire::session_wire_container::resultset_wires_container> create_resultset_wire(std::string_view name) {
        auto rw = wire_.create_resultset_wire();
        try {
            rw->connect(name);
            return rw;
        } catch (std::runtime_error& e) {
            std::cerr << e.what() << std::endl;
        }
        return nullptr;
    }

/**
 * @brief send a request message to fdw service, to use fdw_service.
 * @param request a request message in std::string
 * @return std::optional of std::string
 */
    std::optional<std::string> send(std::string_view request) {
        auto response_opt = send_bridge_request(request);
        if (response_opt) {
            return response_opt.value();
        }
        return std::nullopt;
    }

private:
    tateyama::common::wire::session_wire_container& wire_;
    ::tateyama::proto::framework::request::Header header_{};
    ::tateyama::proto::framework::request::Header bridge_header_{};
    ::jogasaki::proto::sql::common::Session session_{};
    std::string statement_result_{};
    std::string query_result_for_the_one_{};
    std::vector<std::string> query_results_{};
    
    template <typename T>
    std::optional<T> send(::jogasaki::proto::sql::request::Request& request, tateyama::common::wire::message_header::index_type index = 0) {
        std::stringstream ss{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(header_, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        // sql.Request does not have message_version
        // request.set_message_version(MESSAGE_VERSION);
        auto session_handle = request.mutable_session_handle();
        *session_handle = session_;
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(request, std::addressof(ss)); ! res) {
            request.clear_session_handle();
            return std::nullopt;
        }
        wire_.write(ss.str(), index);
        request.clear_session_handle();

        return receive<T>(index);
    }

    template <typename T>
    std::optional<T> receive(tateyama::common::wire::message_header::index_type index) {
        auto& response_wire = wire_.get_response_wire();

        while (true) {
            std::string response_message{};

            if (index == 0 && !statement_result_.empty()) {
                response_message = statement_result_;
                statement_result_.clear();
            } else if (index == OPT_INDEX && !query_result_for_the_one_.empty()) {
                response_message = query_result_for_the_one_;
                query_result_for_the_one_.clear();
            } else if (index > OPT_INDEX && !query_results_at(index).empty()) {
                response_message = query_results_at(index);
                query_results_at(index).clear();
            } else {
                response_wire.await();
                auto slot = response_wire.get_idx();
                if (slot == index) {
                    response_message.resize(response_wire.get_length());
                    response_wire.read(response_message.data());
                } else {
                    if (slot == 0) {
                        statement_result_.resize(response_wire.get_length());
                        response_wire.read(statement_result_.data());
                    } else {
                        std::string& body = (slot == OPT_INDEX) ? query_result_for_the_one_ : query_results_at(slot);
                        body.resize(response_wire.get_length());
                        response_wire.read(body.data());
                    }
                    continue;
                }
            }
            ::tateyama::proto::framework::response::Header header{};
            google::protobuf::io::ArrayInputStream in{response_message.data(), static_cast<int>(response_message.length())};
            if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(header), std::addressof(in), nullptr); ! res) {
                return std::nullopt;
            }
            std::string_view payload{};
            if (auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), nullptr, payload); ! res) {
                return std::nullopt;
            }
            T response{};
            if(auto res = response.ParseFromArray(payload.data(), payload.length()); ! res) {
                return std::nullopt;
            }
            return response;
        }
    }

    std::string& query_results_at(std::size_t index) {
        auto vector_index = index - (OPT_INDEX + 1);
        if (query_results_.size() < (vector_index + 1)) {
            query_results_.resize(vector_index + 1);
        }
        return query_results_.at(vector_index);
    }

    std::optional<std::string> send_bridge_request(std::string_view request, tateyama::common::wire::message_header::index_type index = 0) {
        std::stringstream ss{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(bridge_header_, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        if(auto res = tateyama::utils::PutDelimitedBodyToOstream(request, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        wire_.write(ss.str(), index);

        return receive(index);
    }

    std::optional<std::string> receive(tateyama::common::wire::message_header::index_type index) {
        auto& response_wire = wire_.get_response_wire();

        std::string response_message{};

        response_wire.await();
        auto slot = response_wire.get_idx();
        if (slot == index) {
            response_message.resize(response_wire.get_length());
            response_wire.read(response_message.data());
        }

        ::tateyama::proto::framework::response::Header header{};
        google::protobuf::io::ArrayInputStream in{response_message.data(), static_cast<int>(response_message.length())};
        if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(header), std::addressof(in), nullptr); ! res) {
            return std::nullopt;
        }
        std::string_view response{};
        bool eof;
        if(auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), &eof, response); ! res) {
            return std::nullopt;
        }
        return std::string{response};
    }

    ::jogasaki::proto::sql::request::Request sql_request() {
        ::jogasaki::proto::sql::request::Request request{};
        request.set_service_message_version_major(SQL_MESSAGE_VERSION_MAJOR);
        request.set_service_message_version_minor(SQL_MESSAGE_VERSION_MINOR);
        return request;
    }
};

} // tateyama::bootstrap::wire
