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
    request_ = std::make_unique<ogawayama::common::ChannelStream>("request", managed_shared_memory_ptr, true);
    result_ = std::make_unique<ogawayama::common::ChannelStream>("result", managed_shared_memory_ptr, true);
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
Connection::Connection(Stub *stub, std::size_t pgprocno)
    : impl_(std::make_unique<Connection::Impl>(this, pgprocno)), manager_(stub) {}

}  // namespace ogawayama::stub
