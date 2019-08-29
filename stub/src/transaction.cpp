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
    envelope_->get_manager()->get_impl()->get_channel_streams(request_, result_);
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    for( auto rs : *result_sets_ ) {
        if( rs.use_count() == 1) {
            result_set = rs;
            result_set->get_impl()->clear();
            goto found;
        }
    }
    result_set = std::make_shared<ResultSet>(envelope_, result_sets_->size());
    result_sets_->emplace_back(result_set);

 found:
    request_->get_binary_oarchive() <<
        ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::EXECUTE_QUERY,
                                          static_cast<std::int32_t>(result_set->get_impl()->get_id()),
                                          query);
    Metadata & metadata = result_set->get_impl()->metadata();
    envelope_->get_impl()->get_result_channel()->get_binary_iarchive() >> metadata;
    return ErrorCode::OK;
}

/**
 * @brief execute a statement.
 * @param statement the SQL statement string
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_statement(std::string_view statement) {
    request_->get_binary_oarchive() <<
        ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::EXECUTE_STATEMENT,
                                          0, // dummy
                                          statement);
    ErrorCode reply;
    result_->get_binary_iarchive() >> reply;
    return reply;
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

}  // namespace ogawayama::stub
