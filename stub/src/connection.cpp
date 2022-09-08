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
    std::string name{envelope_->get_manager()->get_impl()->get_shm_name()};
    name += "-" + std::to_string(pgprocno);
    shm4_connection_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_CONNECTION, true, true);
    channel_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::channel, shm4_connection_.get(), true);
    parameters_ = std::make_unique<ogawayama::common::ParameterSet>(ogawayama::common::param::prepared, shm4_connection_.get(), true);

    shm4_row_queue_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_ROW_QUEUE, true, true);
    result_sets_ = std::make_unique<std::vector<std::shared_ptr<ResultSet>>>();
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
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::create_table(std::size_t id)
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::CREATE_TABLE, id);
    return channel_->recv_ack();
}

/**
 * @brief relay a drop tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::drop_table(std::size_t id)
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::DROP_TABLE, id);
    return channel_->recv_ack();
}

/**
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::begin_ddl()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::BEGIN_DDL);
    return channel_->recv_ack();
}

/**
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::end_ddl()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::END_DDL);
    return channel_->recv_ack();
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

/**
 * @brief receive a begin_ddl message from manager
 */
manager::message::Status Connection::receive_begin_ddl(const int64_t mode) const
{
    ErrorCode reply = impl_->begin_ddl();
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a end_ddl message from manager
 */
manager::message::Status Connection::receive_end_ddl() const
{
    ErrorCode reply = impl_->end_ddl();
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a create_table message from manager
 */
manager::message::Status Connection::receive_create_table(const manager::metadata::ObjectIdType object_id) const
{
    ErrorCode reply = impl_->create_table(static_cast<std::size_t>(object_id));
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a drop_table message from manager
 */
manager::message::Status Connection::receive_drop_table(const manager::metadata::ObjectIdType object_id) const
{
    ErrorCode reply = impl_->drop_table(static_cast<std::size_t>(object_id));
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

}  // namespace ogawayama::stub
