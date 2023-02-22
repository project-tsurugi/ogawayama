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

/**
 * @brief execute a prepared statement.
 * @param prepared statement object with parameters
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_statement(PreparedStatement* prepared, const parameters_type& parameters) {
    // FIXME implement
    return ErrorCode::UNSUPPORTED;
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
            auto response_opt = transport_.send(request);
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
                    response.record_meta()
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
ErrorCode Transaction::Impl::execute_query(PreparedStatement* prepared, const parameters_type& parameters, std::shared_ptr<ResultSet> &result_set)
{
    // FIXME implement
    return ErrorCode::UNSUPPORTED;
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
            return ErrorCode::OK;
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
            return ErrorCode::OK;
        }
        return ErrorCode::SERVER_FAILURE;
    }
    return ErrorCode::OK;  // rollback is idempotent
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

ErrorCode Transaction::execute_statement(PreparedStatement* prepared, parameters_type& parameters)
{
    return impl_->execute_statement(prepared, parameters);
}

ErrorCode Transaction::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(query, result_set);
}

ErrorCode Transaction::execute_query(PreparedStatement* prepared, parameters_type& parameters, std::shared_ptr<ResultSet> &result_set)
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
