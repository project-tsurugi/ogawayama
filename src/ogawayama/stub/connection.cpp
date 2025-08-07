/*
 * Copyright 2019-2025 Project Tsurugi.
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

#include <iostream>
#include <exception>

#include <boost/foreach.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS  // to retain the current behavior
#include <boost/property_tree/json_parser.hpp>
#include <boost/archive/basic_archive.hpp> // need for ubuntu 20.04
#include <boost/property_tree/ptree_serialization.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include <ogawayama/common/bridge.h>
#include "ogawayama/transport/tsurugi_error.h"
#include "prepared_statementImpl.h"
#include "search_path_adapter.h"
#include "table_list_adapter.h"
#include "table_metadata_adapter.h"

#include "connectionImpl.h"

namespace ogawayama::stub {

Connection::Impl::Impl(Stub::Impl* manager, std::string_view session_id, std::size_t pgprocno)
    : manager_(manager), session_id_(session_id), wire_(session_id_), transport_(wire_), pgprocno_(pgprocno) {}

Connection::Impl::~Impl()
{
    try {
        transport_.close();
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
    }
}

ErrorCode Connection::Impl::hello() 
{
    auto command = ogawayama::common::command::hello;

    std::ostringstream ofs;
    boost::archive::binary_oarchive oa(ofs);
    oa << common::OGAWAYAMA_MESSAGE_VERSION;
    oa << command;

    try {
        auto response_opt = transport_.send(ofs.str());
        if (response_opt) {
            ERROR_CODE rv{};
            std::istringstream ifs(response_opt.value());
            boost::archive::binary_iarchive ia(ifs);
            ia >> rv;
            return rv;
        }
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::UNKNOWN;
}

ErrorCode Connection::Impl::begin(TransactionPtr& transaction)
{
    try {
        ::jogasaki::proto::sql::request::Begin request{};

        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response_begin = response_opt.value();
        if (response_begin.has_success()) {
            transaction = std::make_unique<Transaction>(std::make_unique<Transaction::Impl>(this, transport_, response_begin.success().transaction_handle()));
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

ErrorCode Connection::Impl::begin(const boost::property_tree::ptree& option, TransactionPtr& transaction)  //NOLINT(readability-function-cognitive-complexity)
{
    ::jogasaki::proto::sql::request::Begin request{};

    auto* topt = request.mutable_option();
    auto ttype = option.get_optional<std::int64_t>(TRANSACTION_TYPE);
    if (ttype) {
        topt->set_type(static_cast<::jogasaki::proto::sql::request::TransactionType>(ttype.value()));
    }
    auto tprio = option.get_optional<std::int64_t>(TRANSACTION_PRIORITY);
    if (tprio) {
        topt->set_priority(static_cast<::jogasaki::proto::sql::request::TransactionPriority>(tprio.value()));
    }
    auto tlabel = option.get_optional<std::string>(TRANSACTION_LABEL);
    if (tlabel) {
        topt->set_label(tlabel.value());
    }
    if (auto nodes = option.get_child_optional(WRITE_PRESERVE); nodes) {
        BOOST_FOREACH (const auto& wp_node, nodes.value()) {
            auto wp = wp_node.second.get_optional<std::string>(TABLE_NAME);
            if (wp) {
                if (auto value = wp.value(); !value.empty()) {
                    topt->add_write_preserves()->set_table_name(value);
                }
            }
        }
    }
    if (auto nodes = option.get_child_optional(INCLUSIVE_READ_AREA); nodes) {
        BOOST_FOREACH (const auto& ra_node, nodes.value()) {
            auto ra = ra_node.second.get_optional<std::string>(TABLE_NAME);
            if (ra) {
                if (auto value = ra.value(); !value.empty()) {
                    topt->add_inclusive_read_areas()->set_table_name(value);
                }
            }
        }
    }
    if (auto nodes = option.get_child_optional(EXCLUSIVE_READ_AREA); nodes) {
        BOOST_FOREACH (const auto& ra_node, nodes.value()) {
            auto ra = ra_node.second.get_optional<std::string>(TABLE_NAME);
            if (ra) {
                if (auto value = ra.value(); !value.empty()) {
                    topt->add_exclusive_read_areas()->set_table_name(value);
                }
            }
        }
    }

    try {
        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response_begin = response_opt.value();
        if (response_begin.has_success()) {
            transaction = std::make_unique<Transaction>(std::make_unique<Transaction::Impl>(this, transport_, response_begin.success().transaction_handle()));
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

ErrorCode Connection::Impl::prepare(std::string_view sql, const placeholders_type& placeholders, PreparedStatementPtr& prepared)
{
    ::jogasaki::proto::sql::request::Prepare request{};

    std::string sql_string(sql);
    request.set_sql(sql_string);

    for (auto& e : placeholders) {
        auto* ph = request.add_placeholders();
        ph->set_name(e.first);
        switch (e.second) {
        case Metadata::ColumnType::Type::INT32:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::INT4);
            break;
        case Metadata::ColumnType::Type::INT64:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::INT8);
            break;
        case Metadata::ColumnType::Type::FLOAT32:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::FLOAT4);
            break;
        case Metadata::ColumnType::Type::FLOAT64:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::FLOAT8);
            break;
        case Metadata::ColumnType::Type::TEXT:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::CHARACTER);
            break;
        case Metadata::ColumnType::Type::DECIMAL:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::DECIMAL);
            break;
        case Metadata::ColumnType::Type::DATE:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::DATE);
            break;
        case Metadata::ColumnType::Type::TIME:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY);
            break;
        case Metadata::ColumnType::Type::TIMESTAMP:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::TIME_POINT);
            break;
        case Metadata::ColumnType::Type::TIMETZ:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE);
            break;
        case Metadata::ColumnType::Type::TIMESTAMPTZ:
            ph->set_atom_type(::jogasaki::proto::sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE);
            break;
        default:
            return ErrorCode::UNSUPPORTED;
        }
    }

    try {
        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response_prepare = response_opt.value();
        if (response_prepare.has_prepared_statement_handle()) {
            auto& psh = response_prepare.prepared_statement_handle();
            std::size_t id = psh.handle();
            bool has_result_records = psh.has_result_records();
            prepared = std::make_unique<PreparedStatement>(std::make_unique<PreparedStatement::Impl>(this, id, has_result_records));
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

ErrorCode Connection::Impl::get_table_metadata(const std::string& table_name, TableMetadataPtr& table_metadata)
{
    try {
        ::jogasaki::proto::sql::request::DescribeTable request{};

        *(request.mutable_name()) = table_name;
        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response_describe_table = response_opt.value();
        if (response_describe_table.has_success()) {
            table_metadata = std::make_unique<table_metadata_adapter>(response_describe_table.success());
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

ErrorCode Connection::Impl::get_list_tables(TableListPtr& table_list) {
    try {
        ::jogasaki::proto::sql::request::ListTables request{};

        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            table_list = std::make_unique<table_list_adapter>(response.success());
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

ErrorCode Connection::Impl::get_search_path(SearchPathPtr& sp) {
    try {
        ::jogasaki::proto::sql::request::GetSearchPath request{};

        auto response_opt = transport_.send(request);
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            sp = std::make_unique<search_path_adapter>(response.success());
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_ERROR;
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_ERROR;
    }
}

static inline bool handle_sql_error(ogawayama::stub::tsurugi_error_code& code, ::jogasaki::proto::sql::response::Error& sql_error) {
    if (auto itr = ogawayama::transport::error_map.find(sql_error.code()); itr != ogawayama::transport::error_map.end()) {
        code.type = tsurugi_error_code::tsurugi_error_type::sql_error;
        code.code = itr->second.second;
        code.name = itr->second.first;
        code.detail = sql_error.detail();
        code.supplemental_text = sql_error.supplemental_text();
        return true;
    }
    return false;
}

static inline bool handle_framework_error(ogawayama::stub::tsurugi_error_code& code, ::tateyama::proto::diagnostics::Record& framework_error) {
    if (auto itr = ogawayama::transport::framework_error_map.find(framework_error.code()); itr != ogawayama::transport::framework_error_map.end()) {
        code.type = tsurugi_error_code::tsurugi_error_type::framework_error;
        code.code = itr->second.second;
        code.name = itr->second.first;
        code.detail = framework_error.message();
        return true;
    }
    return false;
}

ErrorCode Connection::Impl::tsurugi_error(tsurugi_error_code& code)
{
    switch(transport_.last_header().payload_type()) {
    case ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_UNKNOWN:
        break;
    case ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVER_DIAGNOSTICS:
    {
        auto frame_error = transport_.last_framework_error();
        if (handle_framework_error(code, frame_error)) {
            return ErrorCode::OK;
        }
        break;
    }
    case ::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVICE_RESULT:
    {
        auto sql_error = transport_.last_sql_error();
        if (sql_error.code() == ::jogasaki::proto::sql::error::Code::CODE_UNSPECIFIED) {
            code.type = tsurugi_error_code::tsurugi_error_type::none;
            return ErrorCode::OK;
        }
        if (handle_sql_error(code, sql_error)) {
            return ErrorCode::OK;
        }
        break;
    }
    }
    return ErrorCode::UNKNOWN;
}

/**
 * @brief constructor of Connection class
 */
Connection::Connection(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

/**
 * @brief destructor of Connection class
 */
Connection::~Connection() = default;

/**
 * @brief get transaction object, meaning transaction begin
 */
ErrorCode Connection::begin(TransactionPtr &transaction) { return impl_->begin(transaction); }

/**
 * @brief get transaction object, meaning transaction begin
 */
ErrorCode Connection::begin(const boost::property_tree::ptree& option, TransactionPtr &transaction) { return impl_->begin(option, transaction); }

/**
 * @brief prepare statement
 */
ErrorCode Connection::prepare(std::string_view sql, const placeholders_type& placeholders, PreparedStatementPtr &prepared) { return impl_->prepare(sql, placeholders, prepared); }

/**
 * @brief get table metadata
 */
ErrorCode Connection::get_table_metadata(const std::string& table_name, TableMetadataPtr& table_metadata) { return impl_->get_table_metadata(table_name, table_metadata); }

/**
 * @brief get table list
 */
ErrorCode Connection::get_list_tables(TableListPtr& table_list) { return impl_->get_list_tables(table_list); }

/**
 * @brief get search path
 */
ErrorCode Connection::get_search_path(SearchPathPtr& sp) { return impl_->get_search_path(sp); }

/**
 * @brief get the error of the last SQL executed
 */
ErrorCode Connection::tsurugi_error(tsurugi_error_code& code)
{
    return impl_->tsurugi_error(code);
}

}  // namespace ogawayama::stub
