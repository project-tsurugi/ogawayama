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

#include "connectionImpl.h"

namespace ogawayama::stub {

Connection::Impl::Impl(Connection *connection, std::size_t pgprocno) : envelope_(connection), pgprocno_(pgprocno)
{
    boost::interprocess::managed_shared_memory *managed_shared_memory_ptr = envelope_->get_manager()->get_impl()->get_managed_shared_memory_ptr();
    auto managed_shared_memory = envelope_->get_manager()->get_impl()->get_managed_shared_memory();
    channel_ = std::make_unique<ogawayama::common::ChannelStream>
        (managed_shared_memory->shm_name(ogawayama::common::param::channel, pgprocno_).c_str(), managed_shared_memory, true);
    parameters_ = std::make_unique<ogawayama::common::ParameterSet>
        (managed_shared_memory->shm_name(ogawayama::common::param::prepared, pgprocno_).c_str(), managed_shared_memory, true);
    shm4_row_queue_ = std::make_unique<ogawayama::common::SharedMemory>(managed_shared_memory->shm4_row_queue_name(pgprocno), true, true);
}

Connection::Impl::~Impl()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::DISCONNECT);
    channel_->wait();
}

ErrorCode Connection::Impl::confirm()
{
    ErrorCode reply = channel_->recv_ack();
    if (reply != ErrorCode::OK) {
        return ErrorCode::SERVER_FAILURE;
    }
    return reply;
}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Connection::Impl::begin(TransactionPtr &transaction)
{
    transaction = std::make_unique<Transaction>(envelope_);
    return ErrorCode::OK;
}

/**
 * @brief prepare statement
 */
ErrorCode Connection::Impl::prepare(std::string_view sql, PreparedStatementPtr &prepared)
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::PREPARE, sid_, sql);
    ErrorCode reply = channel_->recv_ack();

    if (reply == ErrorCode::OK) {
        prepared = std::make_unique<PreparedStatement>(envelope_, sid_);
        sid_++;
    }
    return reply;
}

/**
 * @brief constructor of Connection class
 */
Connection::Connection(Stub *stub, std::size_t pgprocno) : manager_(stub)
{
    impl_ = std::make_unique<Connection::Impl>(this, pgprocno);
}

/**
 * @brief destructor of Connection class
 */
Connection::~Connection() = default;

/**
 * @brief get transaction object, meaning transaction begin
 */
ErrorCode Connection::begin(TransactionPtr &transaction) { return impl_->begin(transaction); }

/**
 * @brief prepare statement
 */
ErrorCode Connection::prepare(std::string_view sql, PreparedStatementPtr &prepared) { return impl_->prepare(sql, prepared); }

}  // namespace ogawayama::stub
