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

#include "transactionImpl.h"

namespace ogawayama::stub {

Transaction::Impl::Impl(Transaction *transaction) : envelope_(transaction)
{
    result_sets_ = std::make_unique<std::vector<std::shared_ptr<ResultSet>>>();
}

static constexpr std::string_view select_one = "SELECT 1";  // NOLINT
static constexpr std::string_view select_table = "SELECT C1, C2, C3 FROM MOCK WHERE C1 <= 2";  // NOLINT
static constexpr std::string_view update_table = "UPDATE MOCK SET C2=3.3, C3='OPQRSTUVWXYZ' WHERE C1 = 3";  // NOLINT
    
/**
 * @brief execute a statement.
 * @param statement the SQL statement string
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_statement(std::string_view statement) {
    if (statement == update_table) {
        return ErrorCode::OK;
    }
    return ErrorCode::UNKNOWN;
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    result_set = std::make_shared<ResultSet>(envelope_, result_sets_->size());
    result_sets_->emplace_back(result_set);

    MetadataPtr metadata;
    result_set->get_metadata(metadata);
    
    if (query == select_one) {
        metadata->push(TYPE::INT32, 4);
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<std::int32_t>(1));
        result_set->get_impl()->get_row_queue()->push_writing_row();
        return ErrorCode::OK;
    }
    if (query == select_table) {
        metadata->push(TYPE::INT32, 4);
        metadata->push(TYPE::FLOAT64, 8);
        metadata->push(TYPE::TEXT, 16);
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<std::int32_t>(1));
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<double>(1.1));
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<std::string_view>("ABCDE"));
        result_set->get_impl()->get_row_queue()->push_writing_row();
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<std::int32_t>(2));
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<double>(2.2));
        result_set->get_impl()->get_row_queue()->put_next_column(static_cast<std::string_view>("AAABBBCCCDDDEEE"));
        result_set->get_impl()->get_row_queue()->push_writing_row();
        return ErrorCode::OK;
    }
    return ErrorCode::UNKNOWN;
}

/**
 * @brief commit the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::commit()
{
    return ErrorCode::OK;
}

/**
 * @brief abort the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::rollback()
{
    return ErrorCode::OK;
}

/**
 * @brief constructor of Transaction class
 */
Transaction::Transaction(Connection *connection) : manager_(connection)
{
    impl_ = std::make_unique<Transaction::Impl>(this);
}

/**
 * @brief destructor of Transaction class
 */
Transaction::~Transaction() = default;

ErrorCode Transaction::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(query, result_set);
}

ErrorCode Transaction::execute_statement(std::string_view statement)
{
    return impl_->execute_statement(statement);
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
