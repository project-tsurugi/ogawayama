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

#include "stubImpl.h"

namespace ogawayama::stub {

Stub::Impl::Impl(Stub *stub, std::string_view database_name) : envelope_(stub)
{
    shared_memory_ = std::make_unique<ogawayama::common::SharedMemory>(database_name);
    //    server_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory_->get_managed_shared_memory_ptr());
}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Stub::Impl::get_connection(std::size_t pgprocno, ConnectionPtr & connection)
{
    connection = std::make_unique<Connection>(this->envelope_, pgprocno);

    ogawayama::common::CommandMessage message(ogawayama::common::CommandMessage::Type::CONNECT, pgprocno);
    //    server_->lock();
    //    server_->get_binary_oarchive() << message;
    //    server_->unlock();
    return connection->get_impl()->confirm();
}

/**
 * @brief constructor of Stub class
 */
Stub::Stub(std::string_view database_name)
    : impl_(std::make_unique<Stub::Impl>(this, database_name)) {}

/**
 * @brief destructor of Stub class
 */
Stub::~Stub() = default;

/**
 * @brief connect to the DB and get Connection class.
 */
ErrorCode Stub::get_connection(std::size_t pgprocno, ConnectionPtr & connection)
{    
    return impl_->get_connection(pgprocno, connection);
}

}  // namespace ogawayama::stub
