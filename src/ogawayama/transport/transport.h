/*
 * Copyright 2022-2024 Project Tsurugi.
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
#include <exception>
#include <sys/types.h>
#include <unistd.h>

#include <tateyama/utils/protobuf_utils.h>
#include <tateyama/proto/framework/request.pb.h>
#include <tateyama/proto/framework/response.pb.h>
#include <tateyama/proto/core/request.pb.h>
#include <tateyama/proto/core/response.pb.h>
#include <tateyama/proto/endpoint/request.pb.h>
#include <tateyama/proto/endpoint/response.pb.h>

#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/response.pb.h>

#include <ogawayama/stub/error_code.h>

#include "tateyama/authentication/authentication.h"
#include "tateyama/transport/client_wire.h"
#include "tateyama/transport/timer.h"

namespace tateyama::bootstrap::wire {

class transport {
    constexpr static std::size_t HEADER_MESSAGE_VERSION_MAJOR = 0;
    constexpr static std::size_t HEADER_MESSAGE_VERSION_MINOR = 1;
    constexpr static std::size_t CORE_MESSAGE_VERSION_MAJOR = 0;
    constexpr static std::size_t CORE_MESSAGE_VERSION_MINOR = 0;
    constexpr static std::size_t SQL_MESSAGE_VERSION_MAJOR = 2;
    constexpr static std::size_t SQL_MESSAGE_VERSION_MINOR = 0;
    constexpr static std::size_t ENDPOINT_MESSAGE_VERSION_MAJOR = 0;
    constexpr static std::size_t ENDPOINT_MESSAGE_VERSION_MINOR = 0;
    constexpr static std::uint32_t SERVICE_ID_ROUTING = 0;  // from tateyama/framework/component_ids.h
    constexpr static std::uint32_t SERVICE_ID_ENDPOINT_BROKER = 1;  // from tateyama/framework/component_ids.h
    constexpr static std::uint32_t SERVICE_ID_SQL = 3;  // from tateyama/framework/component_ids.h
    constexpr static std::uint32_t SERVICE_ID_FDW = 4;  // from tateyama/framework/component_ids.h
    constexpr static std::uint32_t EXPIRATION_SECONDS = 60;

public:
    transport() = delete;

    explicit transport(tateyama::common::wire::session_wire_container& wire) : wire_(wire) {
        header_.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        header_.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        header_.set_service_id(SERVICE_ID_SQL);
        bridge_header_.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        bridge_header_.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        bridge_header_.set_service_id(SERVICE_ID_FDW);
        auto handshake_response = handshake();
        if (!handshake_response) {
            throw std::runtime_error("handshake error");
        }
        if (handshake_response.value().result_case() != tateyama::proto::endpoint::response::Handshake::ResultCase::kSuccess) {
            throw std::runtime_error("handshake error");
        }

        timer_ = std::make_unique<tateyama::common::wire::timer>(EXPIRATION_SECONDS, [this](){
            auto ret = update_expiration_time();
            if (ret.has_value()) {
                return ret.value().result_case() == tateyama::proto::core::response::UpdateExpirationTime::ResultCase::kSuccess;
            }
            return false;
        });
    }

    ~transport() {
        try {
            timer_ = nullptr;
            if (!closed_) {
                close();
            }
        } catch (std::exception &ex) {
            std::cerr << ex.what() << std::endl;
        }
    }

    transport(transport const& other) = delete;
    transport& operator=(transport const& other) = delete;
    transport(transport&& other) noexcept = delete;
    transport& operator=(transport&& other) noexcept = delete;

/**
 * @brief send a begin request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::Begin
 */
    std::optional<::jogasaki::proto::sql::response::Begin> send(::jogasaki::proto::sql::request::Begin& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_begin())= req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_begin();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_begin()) {
                auto response = response_message.begin();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                return response;
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a prepare request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::Prepare
 */
    std::optional<::jogasaki::proto::sql::response::Prepare> send(::jogasaki::proto::sql::request::Prepare& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_prepare())= req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_prepare();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_prepare()) {
                auto response = response_message.prepare();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                return response;
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute statement request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ExecuteResult
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteResult> send(::jogasaki::proto::sql::request::ExecuteStatement& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_execute_statement()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_execute_statement();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_result()) {
                auto response = response_message.execute_result();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                return response;
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute query request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ExecuteQuery
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteQuery> send(::jogasaki::proto::sql::request::ExecuteQuery& req, tateyama::common::wire::message_header::index_type& query_index) {
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_execute_query()) = req;

        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, query_index);
        request.clear_execute_query();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_query()) {
                return response_message.execute_query();
            }
            if (response_message.has_result_only()) {
                const auto& response = response_message.result_only();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                throw std::runtime_error("no body_head message");
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute prepared statement request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ExecuteResult
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteResult> send(::jogasaki::proto::sql::request::ExecutePreparedStatement& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_execute_prepared_statement()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_execute_prepared_statement();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_result()) {
                auto response = response_message.execute_result();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                return response;
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a execute prepared query request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ExecutePreparedQuery
 */
    std::optional<::jogasaki::proto::sql::response::ExecuteQuery> send(::jogasaki::proto::sql::request::ExecutePreparedQuery& req, tateyama::common::wire::message_header::index_type& query_index) {
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_execute_prepared_query()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, query_index);
        request.clear_execute_prepared_query();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_execute_query()) {
                return response_message.execute_query();
            }
            if (response_message.has_result_only()) {
                const auto& response = response_message.result_only();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
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
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ResultOnly> send(::jogasaki::proto::sql::request::Commit& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_commit()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_commit();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                auto response = response_message.result_only();
                sql_error_ = response.has_error() ? response.error() : ::jogasaki::proto::sql::response::Error{};
                return response;
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a rollback request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::ResultOnly> send(::jogasaki::proto::sql::request::Rollback& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_rollback()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
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
 * @brief send a dispose prepared statement request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<ogawayama::stub::ErrorCode> send(::jogasaki::proto::sql::request::DisposePreparedStatement& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_dispose_prepared_statement()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_dispose_prepared_statement();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                const auto& ro = response_message.result_only();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
            if (response_message.has_dispose_transaction()) {
                const auto& ro = response_message.dispose_transaction();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
        }
        return std::nullopt;
    }

// Explain
// ExecuteDump
// ExecuteLoad

/**
 * @brief send a describe table request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::response::DescribeTable
 */
    std::optional<::jogasaki::proto::sql::response::DescribeTable> send(::jogasaki::proto::sql::request::DescribeTable& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_describe_table()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_describe_table();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_describe_table()) {
                return response_message.describe_table();
            }
        }
        return std::nullopt;
    }

// Batch

/**
 * @brief send a list table request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::response::DescribeTable
 */
    std::optional<::jogasaki::proto::sql::response::ListTables> send(::jogasaki::proto::sql::request::ListTables& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_listtables()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_listtables();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_list_tables()) {
                return response_message.list_tables();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a get serch path request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::response::DescribeTable
 */
    std::optional<::jogasaki::proto::sql::response::GetSearchPath> send(::jogasaki::proto::sql::request::GetSearchPath& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_getsearchpath()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_getsearchpath();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_get_search_path()) {
                return response_message.get_search_path();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a get error info request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::response::DescribeTable
 */
    std::optional<::jogasaki::proto::sql::response::GetErrorInfo> send(::jogasaki::proto::sql::request::GetErrorInfo& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_get_error_info()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_get_error_info();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_get_error_info()) {
                return response_message.get_error_info();
            }
        }
        return std::nullopt;
    }

/**
 * @brief send a dispose transaction request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<ogawayama::stub::ErrorCode> send(::jogasaki::proto::sql::request::DisposeTransaction& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_dispose_transaction()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_dispose_transaction();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_result_only()) {
                const auto& ro = response_message.result_only();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
            if (response_message.has_dispose_transaction()) {
                const auto& ro = response_message.dispose_transaction();
                if (ro.has_success()) {
                    return ogawayama::stub::ErrorCode::OK;
                }
            }
        }
        return std::nullopt;
    }

// ExplainByText
// ExtractStatementInfo

/**
 * @brief send a get large object data request to the sql service.
 * @param req the request message by protocol buffers
 * @return std::optional of ::jogasaki::proto::sql::request::ResultOnly
 */
    std::optional<::jogasaki::proto::sql::response::GetLargeObjectData> send(::jogasaki::proto::sql::request::GetLargeObjectData& req) {
        tateyama::common::wire::message_header::index_type slot_index{};
        ::jogasaki::proto::sql::request::Request request{};
        *(request.mutable_get_large_object_data()) = req;
        auto response_opt = send<::jogasaki::proto::sql::response::Response>(request, slot_index);
        request.clear_get_large_object_data();
        if (response_opt) {
            auto response_message = response_opt.value();
            if (response_message.has_get_large_object_data()) {
                return response_message.get_large_object_data();
            }
        }
        return std::nullopt;
    }

    void close() {
        wire_.close();
        closed_ = true;
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

    // used only by connection
    ::tateyama::proto::framework::response::Header& last_header() {
        return response_header_;
    }
    ::jogasaki::proto::sql::response::Error& last_sql_error() {
        return sql_error_;
    }
    ::tateyama::proto::diagnostics::Record& last_framework_error() {
        return framework_error_;
    }

private:
    tateyama::common::wire::session_wire_container& wire_;
    ::tateyama::proto::framework::request::Header header_{};
    ::tateyama::proto::framework::request::Header bridge_header_{};
    ::jogasaki::proto::sql::common::Session session_{};
    std::string statement_result_{};
    std::string query_result_for_the_one_{};
    std::vector<std::string> query_results_{};
    bool closed_{};
    std::unique_ptr<tateyama::common::wire::timer> timer_{};
    std::string encrypted_credential_{};
    ::tateyama::proto::framework::response::Header response_header_{};
    ::jogasaki::proto::sql::response::Error sql_error_{};
    ::tateyama::proto::diagnostics::Record framework_error_{};

    template <typename T>
    std::optional<T> send(::jogasaki::proto::sql::request::Request& request, tateyama::common::wire::message_header::index_type& slot_index) {
        std::stringstream ss{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(header_, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        request.set_service_message_version_major(SQL_MESSAGE_VERSION_MAJOR);
        request.set_service_message_version_minor(SQL_MESSAGE_VERSION_MINOR);
        auto session_handle = request.mutable_session_handle();
        *session_handle = session_;
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(request, std::addressof(ss)); ! res) {
            request.clear_session_handle();
            return std::nullopt;
        }
        slot_index = wire_.search_slot();
        wire_.send(ss.str(), slot_index);
        request.clear_session_handle();

        return receive<T>(slot_index);
    }

    template <typename T>
    std::optional<T> send(::tateyama::proto::core::request::Request& request) {
        tateyama::proto::framework::request::Header fwrq_header{};
        fwrq_header.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        fwrq_header.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        fwrq_header.set_service_id(SERVICE_ID_ROUTING);

        std::stringstream sst{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(fwrq_header, std::addressof(sst)); ! res) {
            return std::nullopt;
        }
        request.set_service_message_version_major(CORE_MESSAGE_VERSION_MAJOR);
        request.set_service_message_version_minor(CORE_MESSAGE_VERSION_MINOR);
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(request, std::addressof(sst)); ! res) {
            return std::nullopt;
        }
        auto slot_index = wire_.search_slot();
        wire_.send(sst.str(), slot_index);
        return receive<T>(slot_index);
    }

    template <typename T>
    std::optional<T> send(::tateyama::proto::endpoint::request::Request& request) {
        tateyama::proto::framework::request::Header fwrq_header{};
        fwrq_header.set_service_message_version_major(HEADER_MESSAGE_VERSION_MAJOR);
        fwrq_header.set_service_message_version_minor(HEADER_MESSAGE_VERSION_MINOR);
        fwrq_header.set_service_id(SERVICE_ID_ENDPOINT_BROKER);

        std::stringstream sst{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(fwrq_header, std::addressof(sst)); ! res) {
            return std::nullopt;
        }
        request.set_service_message_version_major(ENDPOINT_MESSAGE_VERSION_MAJOR);
        request.set_service_message_version_minor(ENDPOINT_MESSAGE_VERSION_MINOR);
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(request, std::addressof(sst)); ! res) {
            return std::nullopt;
        }
        auto slot_index = wire_.search_slot();
        wire_.send(sst.str(), slot_index);
        return receive<T>(slot_index);
    }

    template <typename T>
    std::optional<T> receive(tateyama::common::wire::message_header::index_type slot_index) { //NOLINT(readability-function-cognitive-complexity)
        while (true) {
            std::string response_message{};
            wire_.receive(response_message, slot_index);

            response_header_ = ::tateyama::proto::framework::response::Header{};
            google::protobuf::io::ArrayInputStream in{response_message.data(), static_cast<int>(response_message.length())};
            if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(response_header_), std::addressof(in), nullptr); ! res) {
                return std::nullopt;
            }
            if (response_header_.payload_type() == ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVER_DIAGNOSTICS) {
                std::string_view record{};
                if (auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), nullptr, record); ! res) {
                    return std::nullopt;
                }
                if(auto res = framework_error_.ParseFromArray(record.data(), static_cast<int>(record.length())); ! res) {
                    return std::nullopt;
                }
                throw std::runtime_error("received SERVER_DIAGNOSTICS");
            }
            if (response_header_.payload_type() != ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT) {
                throw std::runtime_error("unknown payload type");
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

    std::string& query_results_at(std::size_t slot_index) {
        return query_results_.at(slot_index);
    }

    std::optional<std::string> send_bridge_request(std::string_view request) {
        std::stringstream ss{};
        if(auto res = tateyama::utils::SerializeDelimitedToOstream(bridge_header_, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        if(auto res = tateyama::utils::PutDelimitedBodyToOstream(request, std::addressof(ss)); ! res) {
            return std::nullopt;
        }
        auto slot_index = wire_.search_slot();
        wire_.send(ss.str(), slot_index);

        return receive(slot_index);
    }

    std::optional<tateyama::proto::endpoint::response::Handshake> handshake() {
        tateyama::proto::endpoint::request::Request request{};
        auto* handshake = request.mutable_handshake();
        auto* client_information = handshake->mutable_client_information();
        auto* wire_information = handshake->mutable_wire_information();
        auto* ipc_information = wire_information->mutable_ipc_information();

        tateyama::authentication::add_credential(*client_information, [this](){
            auto key_opt = encryption_key();
            if (key_opt) {
                const auto& key = key_opt.value();
                if (key.result_case() == tateyama::proto::endpoint::response::EncryptionKey::ResultCase::kSuccess) {
                    return std::optional<std::string>{key.success().encryption_key()};
                }
                if (key.error().code() == tateyama::proto::diagnostics::Code::UNSUPPORTED_OPERATION) {
                    return std::optional<std::string>{std::nullopt};
                }
            }
            throw std::runtime_error("error in get encryption key");
        });

        if (client_information->credential().credential_opt_case() == tateyama::proto::endpoint::request::Credential::kEncryptedCredential) {
            encrypted_credential_ = client_information->credential().encrypted_credential();
        }
        client_information->set_application_name("fdw");
        ipc_information->set_connection_information(std::to_string(getpid()));

        return send<tateyama::proto::endpoint::response::Handshake>(request);
    }

    // EncryptionKey
    std::optional<tateyama::proto::endpoint::response::EncryptionKey> encryption_key() {
        tateyama::proto::endpoint::request::Request request{};
        (void) request.mutable_encryption_key();

        return send<tateyama::proto::endpoint::response::EncryptionKey>(request);
    }

    std::optional<tateyama::proto::core::response::UpdateExpirationTime> update_expiration_time() {
        tateyama::proto::core::request::UpdateExpirationTime uet_request{};

        tateyama::proto::core::request::Request request{};
        *(request.mutable_update_expiration_time()) = uet_request;

        return send<tateyama::proto::core::response::UpdateExpirationTime>(request);
    }

    std::optional<std::string> receive(tateyama::common::wire::message_header::index_type slot_index) {
        std::string response_message{};
        wire_.receive(response_message, slot_index);

        response_header_ = ::tateyama::proto::framework::response::Header{};
        google::protobuf::io::ArrayInputStream in{response_message.data(), static_cast<int>(response_message.length())};
        if(auto res = tateyama::utils::ParseDelimitedFromZeroCopyStream(std::addressof(response_header_), std::addressof(in), nullptr); ! res) {
            return std::nullopt;
        }
        if (response_header_.payload_type() == ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVER_DIAGNOSTICS) {
            std::string_view record{};
            if (auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), nullptr, record); ! res) {
                return std::nullopt;
            }
            if(auto res = framework_error_.ParseFromArray(record.data(), static_cast<int>(record.length())); ! res) {
                return std::nullopt;
            }
            throw std::runtime_error("received SERVER_DIAGNOSTICS");
        }
        if (response_header_.payload_type() != ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT) {
            throw std::runtime_error("unknown payload type");
        }
        std::string_view response{};
        bool eof{};
        if(auto res = tateyama::utils::GetDelimitedBodyFromZeroCopyStream(std::addressof(in), &eof, response); ! res) {
            return std::nullopt;
        }
        return std::string{response};
    }
};

} // tateyama::bootstrap::wire
