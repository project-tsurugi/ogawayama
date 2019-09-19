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
    request_ = std::make_unique<ogawayama::common::ChannelStream>(envelope_->get_manager()->get_impl()->get_managed_shared_memory()->shm_name(ogawayama::common::param::request, pgprocno_).c_str(), managed_shared_memory_ptr, true);
    result_ = std::make_unique<ogawayama::common::ChannelStream>(envelope_->get_manager()->get_impl()->get_managed_shared_memory()->shm_name(ogawayama::common::param::result, pgprocno_).c_str(), managed_shared_memory_ptr, true);
}

Connection::Impl::~Impl()
{
    request_->send(ogawayama::common::CommandMessage::Type::DISCONNECT);
    request_->wait();
}

ErrorCode Connection::Impl::confirm()
{
    ErrorCode reply;
    result_->recv(reply);
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
 * @brief destructor of Stub class
 */
ErrorCode Connection::begin(std::unique_ptr<Transaction> &transaction) { return impl_->begin(transaction); }

}  // namespace ogawayama::stub
