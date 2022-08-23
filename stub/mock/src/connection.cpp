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
}

Connection::Impl::~Impl()
{
}

ErrorCode Connection::Impl::confirm()
{
    ErrorCode reply = ErrorCode::OK;
    if (reply != ErrorCode::OK) {
        std::cerr << "recieved an illegal message" << std::endl;
        exit(-1);
    }
    return reply;
}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Connection::Impl::begin(std::unique_ptr<Transaction> &transaction)
{
    transaction = std::make_unique<Transaction>(envelope_);
    return ErrorCode::OK;
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
 * @brief begin a transaction
 * *param transaction returns a transaction class
 * @return error code defined in error_code.h
 */
ErrorCode Connection::begin(std::unique_ptr<Transaction> &transaction) { return impl_->begin(transaction); }

manager::message::Status Connection::receive_begin_ddl(int64_t mode) const
{
    return manager::message::Status(manager::message::ErrorCode::FAILURE, static_cast<int>(ErrorCode::UNSUPPORTED));
}

/**
 * @brief receive a end_ddl message from manager
 */
manager::message::Status Connection::receive_end_ddl() const
{
    return manager::message::Status(manager::message::ErrorCode::FAILURE, static_cast<int>(ErrorCode::UNSUPPORTED));
}

/**
 * @brief implements receive_create_table() procedure
 * @return Status defined in message-broker/include/manager/message/status.h
 */
manager::message::Status Connection::receive_create_table(metadata::ObjectIdType object_id) const
{
    return manager::message::Status(manager::message::ErrorCode::FAILURE, static_cast<int>(ErrorCode::UNSUPPORTED));
}

/**
 * @brief implements drop_table() procedure
 * @return Status defined in message-broker/include/manager/message/status.h
 */
manager::message::Status Connection::receive_drop_table(metadata::ObjectIdType object_id) const
{
    return manager::message::Status(manager::message::ErrorCode::FAILURE, static_cast<int>(ErrorCode::UNSUPPORTED));
}


}  // namespace ogawayama::stub
