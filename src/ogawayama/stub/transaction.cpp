/*
 * Copyright 2019-2023 tsurugi project.
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

#include "prepared_statementImpl.h"
#include "transactionImpl.h"

namespace ogawayama::stub {

Transaction::Impl::Impl(Connection::Impl* manager, tateyama::bootstrap::wire::transport& transport, ::jogasaki::proto::sql::common::Transaction transaction_handle)
    : manager_(manager), transport_(transport), transaction_handle_(transaction_handle)
{
}

Transaction::Impl::~Impl()
{
    if (alive_) {
        rollback();
        alive_ = false;
    }
}

/**
 * @brief execute a statement.
 * @param statement the SQL statement string
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_statement(std::string_view statement) {
    if (alive_) {
        ::jogasaki::proto::sql::request::ExecuteStatement request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;
        *(request.mutable_sql()) = statement;
        auto response_opt = transport_.send(request);
        request.clear_sql();
        request.clear_transaction_handle();
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::NO_TRANSACTION;
}

class parameter {
public:
    parameter(std::string name) {
        parameter_.set_name(name);
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const std::monostate& data) {
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const std::int32_t& data) {
        parameter_.set_int4_value(data);
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const std::int64_t& data) {
        parameter_.set_int8_value(data);
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const float& data) {
        parameter_.set_float4_value(data);
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const double& data) {
        parameter_.set_float8_value(data);
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const std::string& data) {
        parameter_.set_character_value(data);
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const date_type& data) {
        parameter_.set_date_value(data.days_since_epoch());
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const time_type& data) {
        parameter_.set_time_of_day_value(data.time_since_epoch().count());
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const timestamp_type& data) {
        auto v = parameter_.mutable_time_point_value();
        v->set_offset_seconds(data.seconds_since_epoch().count());
        v->set_nano_adjustment(data.subsecond().count());
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const timetz_type& data) {
        auto v = parameter_.mutable_time_of_day_with_time_zone_value();
        v->set_time_zone_offset(data.second);
        v->set_offset_nanoseconds(data.first.time_since_epoch().count());
        return parameter_;
    }
    ::jogasaki::proto::sql::request::Parameter operator()(const timestamptz_type& data) {
        auto v = parameter_.mutable_time_point_with_time_zone_value();
        v->set_time_zone_offset(data.second);
        v->set_offset_seconds(data.first.seconds_since_epoch().count());
        v->set_nano_adjustment(data.first.subsecond().count());
        return parameter_;
    }

private:
    ::jogasaki::proto::sql::request::Parameter parameter_{};
};

/**
 * @brief execute a prepared statement.
 * @param prepared statement object with parameters
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_statement(PreparedStatementPtr& prepared, const parameters_type& parameters) {
    if (alive_) {
        auto* ps_impl = prepared->get_impl();

        if (ps_impl->has_result_records()) {
            return ErrorCode::INVALID_PARAMETER;
        }

        ::jogasaki::proto::sql::request::ExecutePreparedStatement request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;

        ::jogasaki::proto::sql::common::PreparedStatement prepaed_statement{};
        prepaed_statement.set_handle(ps_impl->get_id());
        prepaed_statement.set_has_result_records(ps_impl->has_result_records());
        request.set_allocated_prepared_statement_handle(&prepaed_statement);

        for (auto& e : parameters) {
            *(request.add_parameters()) = std::visit(parameter(e.first), e.second);
        }
        auto response_opt = transport_.send(request);
        request.clear_parameters();
        request.release_prepared_statement_handle();
        request.clear_transaction_handle();
        
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::NO_TRANSACTION;
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    if (alive_) {
        ::jogasaki::proto::sql::request::ExecuteQuery request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;
        *(request.mutable_sql()) = query;
        try {
            std::size_t query_index = get_query_index();
            auto response_opt = transport_.send(request, query_index);
            request.clear_sql();
            request.clear_transaction_handle();
            if (!response_opt) {
                return ErrorCode::SERVER_FAILURE;
            }
            auto response = response_opt.value();
            result_set = std::make_shared<ResultSet>(
                std::make_unique<ResultSet::Impl>(
                    this,
                    transport_.create_resultset_wire(response.name()),
                    response.record_meta(),
                    query_index
                )
            );
            return ErrorCode::OK;
        } catch (std::runtime_error &e) {
            return ErrorCode::SERVER_FAILURE;
        }
    }
    return ErrorCode::NO_TRANSACTION;
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_query(PreparedStatementPtr& prepared, const parameters_type& parameters, std::shared_ptr<ResultSet> &result_set)
{
    if (alive_) {
        auto* ps_impl = prepared->get_impl();

        if (!ps_impl->has_result_records()) {
            return ErrorCode::INVALID_PARAMETER;
        }
        
        ::jogasaki::proto::sql::request::ExecutePreparedQuery request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;

        ::jogasaki::proto::sql::common::PreparedStatement prepaed_statement{};
        prepaed_statement.set_handle(ps_impl->get_id());
        prepaed_statement.set_has_result_records(ps_impl->has_result_records());
        request.set_allocated_prepared_statement_handle(&prepaed_statement);
        try {
            for (auto& e : parameters) {
                *(request.add_parameters()) = std::visit(parameter(e.first), e.second);
            }
            std::size_t query_index = get_query_index();
            auto response_opt = transport_.send(request, query_index);
            request.clear_parameters();
            request.release_prepared_statement_handle();
            request.clear_transaction_handle();
            if (!response_opt) {
                return ErrorCode::SERVER_FAILURE;
            }
            auto response = response_opt.value();
            result_set = std::make_shared<ResultSet>(
                std::make_unique<ResultSet::Impl>(
                    this,
                    transport_.create_resultset_wire(response.name()),
                    response.record_meta(),
                    query_index
                )
            );
            return ErrorCode::OK;
        } catch (std::runtime_error &e) {
            return ErrorCode::SERVER_FAILURE;
        }
    }
    return ErrorCode::NO_TRANSACTION;
}

/**
 * @brief commit the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::commit()
{
    if (alive_) {
        ::jogasaki::proto::sql::request::Commit request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;
        auto response_opt = transport_.send(request);
        request.clear_transaction_handle();
        alive_ = false;
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            return dispose_transaction();
        }
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::NO_TRANSACTION;
}

/**
 * @brief abort the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::rollback()
{
    if (alive_) {
        ::jogasaki::proto::sql::request::Rollback request{};
        *(request.mutable_transaction_handle()) = transaction_handle_;
        auto response_opt = transport_.send(request);
        request.clear_transaction_handle();
        alive_ = false;
        if (!response_opt) {
            return ErrorCode::SERVER_FAILURE;
        }
        auto response = response_opt.value();
        if (response.has_success()) {
            return dispose_transaction();
        }
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::OK;  // rollback is idempotent
}

ErrorCode Transaction::Impl::dispose_transaction()
{
    ::jogasaki::proto::sql::request::DisposeTransaction request{};
    *(request.mutable_transaction_handle()) = transaction_handle_;
    auto response_opt = transport_.send(request);
    request.clear_transaction_handle();
    alive_ = false;
    if (!response_opt) {
        return ErrorCode::SERVER_FAILURE;
    }
    return response_opt.value();
}

/**
 * @brief constructor of Transaction class
 */
Transaction::Transaction(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

/**
 * @brief destructor of Transaction class
 */
Transaction::~Transaction() = default;

ErrorCode Transaction::execute_statement(std::string_view statement)
{
    return impl_->execute_statement(statement);
}

ErrorCode Transaction::execute_statement(PreparedStatementPtr& prepared, parameters_type& parameters)
{
    return impl_->execute_statement(prepared, parameters);
}

ErrorCode Transaction::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(query, result_set);
}

ErrorCode Transaction::execute_query(PreparedStatementPtr& prepared, parameters_type& parameters, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(prepared, parameters, result_set);
}

ErrorCode Transaction::commit()
{
    return impl_->commit();
}

ErrorCode Transaction::rollback()
{
    return impl_->rollback();
}

}  // namespace ogawayama::stub
