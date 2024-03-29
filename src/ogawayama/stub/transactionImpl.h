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
#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <stdexcept>

#include "connectionImpl.h"
#include "result_setImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Transaction::Impl class
 */
class Transaction::Impl
{
public:
    Impl(Connection::Impl*, tateyama::bootstrap::wire::transport&, ::jogasaki::proto::sql::common::Transaction);
    ~Impl();

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @param num_rows a reference to a variable to which the number of processes
     * @return true in error, otherwise false
     */
    ErrorCode execute_statement(std::string_view statement, std::size_t& num_rows);

    /**
     * @brief execute a prepared statement.
     * @param pointer to the prepared statement
     * @param the parameters to be used for execution of the prepared statement
     * @param num_rows a reference to a variable to which the number of processes
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(PreparedStatementPtr& prepared_statement, const parameters_type& parameters, std::size_t& num_rows);

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return true in error, otherwise false
     */
    ErrorCode execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set);

    /**
     * @brief execute a query.
     * @param pointer to the prepared statement
     * @param the parameters to be used for execution of the prepared statement
     * @param result_set returns a result set of the query
     * @return true in error, otherwise false
     */
    ErrorCode execute_query(PreparedStatementPtr& prepared_statement, const parameters_type& parameters, std::shared_ptr<ResultSet> &result_set);

    /**
     * @brief commit the current transaction.
     * @return error code defined in error_code.h
     */
    ErrorCode commit();
    
    /**
     * @brief abort the current transaction.
     * @return error code defined in error_code.h
     */
    ErrorCode rollback();

private:
    Connection::Impl* manager_;
    tateyama::bootstrap::wire::transport& transport_;
    ::jogasaki::proto::sql::common::Transaction transaction_handle_;

    constexpr static std::size_t opt_index = 2;  // reserve 0 for commit and 1 for dispose_transaction
    std::size_t max_query_index_{opt_index};
    std::queue<std::size_t> query_index_queue_{};
    bool query_in_processing_{false};
    bool alive_{true};

    /**
     * @brief get the object to which this belongs
     * @return stub objext
     */
    auto get_manager() { return manager_; }

    void receive_body(std::size_t query_index) {
        put_query_index(query_index);
        auto body_opt = transport_.receive_body(query_index);
        if (!body_opt) {
            std::cerr << "error at " << __func__ << std::endl;  // NOLINT FIXME revise error handling
        }
    }

    std::size_t get_query_index() {
        if (!query_in_processing_) {
            query_in_processing_ = true;
            return opt_index;
        }
        if (!query_index_queue_.empty()) {
            std::size_t rv = query_index_queue_.front();
            query_index_queue_.pop();
            return rv;
        }
        return ++max_query_index_;
    }
    void put_query_index(std::size_t query_index) {
        if (query_index == opt_index) {
            query_in_processing_ = false;
        } else {
            query_index_queue_.push(query_index);
        }
    }
    ErrorCode dispose_transaction();

    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub
