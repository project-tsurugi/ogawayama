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

#include "prepared_statementImpl.h"
#include "transactionImpl.h"
#include "ogawayama/stub/CreateTableCommand.h"

namespace ogawayama::stub {

Transaction::Impl::Impl(Transaction *transaction) : envelope_(transaction)
{
    envelope_->get_manager()->get_impl()->get_channel_stream(channel_);
    envelope_->get_manager()->get_impl()->get_result_sets(result_sets_);
}

Transaction::Impl::~Impl()
{
    for (auto& rs : *result_sets_) {
        if( rs.use_count() == 1) {
            rs = nullptr;
        }
    }
}

/**
 * @brief execute a statement.
 * @param statement the SQL statement string
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_statement(std::string_view statement) {
    channel_->send_req(ogawayama::common::CommandMessage::Type::EXECUTE_STATEMENT, 1, statement);
    ErrorCode reply = channel_->recv_ack();
    return reply;
}

/**
 * @brief execute a prepared statement.
 * @param prepared statement object with parameters
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_statement(PreparedStatement* prepared) {
    channel_->send_req(ogawayama::common::CommandMessage::Type::EXECUTE_PREPARED_STATEMENT, prepared->get_impl()->get_sid());
    ErrorCode reply = channel_->recv_ack();
    prepared->get_impl()->clear();
    return reply;
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    for (auto& rs : *result_sets_) {
        if(rs == nullptr) {
            result_set = std::make_shared<ResultSet>(envelope_, &rs - &(*result_sets_)[0]);
            rs = result_set;
            goto found;
        }
        if(rs.use_count() == 1) {
            result_set = rs;
            goto found;
        }
    }
    result_set = std::make_shared<ResultSet>(envelope_, result_sets_->size());
    result_sets_->emplace_back(result_set);
 found:
    result_set->get_impl()->set_bulk_transfer_mode();
    channel_->send_req(ogawayama::common::CommandMessage::Type::EXECUTE_QUERY,
                   static_cast<std::int32_t>(result_set->get_impl()->get_id()),
                   query);
    ErrorCode reply = channel_->recv_ack();
    return reply;
}

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::execute_query(PreparedStatement* prepared, std::shared_ptr<ResultSet> &result_set)
{
    for (auto& rs : *result_sets_) {
        if(rs == nullptr) {
            result_set = std::make_shared<ResultSet>(envelope_, &rs - &(*result_sets_)[0]);
            rs = result_set;
            goto found;
        }
        if(rs.use_count() == 1) {
            result_set = rs;
            goto found;
        }
    }
    result_set = std::make_shared<ResultSet>(envelope_, result_sets_->size());
    result_sets_->emplace_back(result_set);
 found:
    result_set->get_impl()->set_bulk_transfer_mode();
    channel_->send_req(ogawayama::common::CommandMessage::Type::EXECUTE_PREPARED_QUERY,
                       prepared->get_impl()->get_sid(),
                       std::to_string(result_set->get_impl()->get_id()));
    ErrorCode reply = channel_->recv_ack();
    prepared->get_impl()->clear();
    return reply;
}

/**
 * @brief commit the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::commit()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::COMMIT);
    ErrorCode reply = channel_->recv_ack();
    return reply;
}

/**
 * @brief abort the current transaction.
 * @return error code defined in error_code.h
 */
ErrorCode Transaction::Impl::rollback()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::ROLLBACK);
    ErrorCode reply = channel_->recv_ack();
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

ErrorCode Transaction::execute_statement(std::string_view statement)
{
    return impl_->execute_statement(statement);
}

ErrorCode Transaction::execute_statement(PreparedStatement* prepared)
{
    return impl_->execute_statement(prepared);
}

ErrorCode Transaction::execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(query, result_set);
}

ErrorCode Transaction::execute_query(PreparedStatement* prepared, std::shared_ptr<ResultSet> &result_set)
{
    return impl_->execute_query(prepared, result_set);
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
