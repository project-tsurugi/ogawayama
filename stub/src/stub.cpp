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

#include "boost/container/vector.hpp"

#include "connectionImpl.h"

#include "stubImpl.h"

namespace ogawayama::stub {

Stub::Impl::Impl(Stub *stub, std::string_view database_name, bool create_shm) : envelope_(stub), database_name_(database_name)
{
    if (create_shm) {
        boost::interprocess::shared_memory_object::remove(database_name_.c_str());
        managed_shared_memory_ = boost::interprocess::managed_shared_memory(boost::interprocess::create_only, database_name_.c_str(), SEGMENT_SIZE);
        server_ = std::make_unique<ogawayama::common::ChannelStream>("server", &managed_shared_memory_, true);
    } else {
        managed_shared_memory_ = boost::interprocess::managed_shared_memory(boost::interprocess::open_only, database_name_.c_str());
        assert (managed_shared_memory_.get_size() == SEGMENT_SIZE);
        server_ = std::make_unique<ogawayama::common::ChannelStream>("server", &managed_shared_memory_);
    }
}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Stub::Impl::get_connection(std::size_t pgprocno, ConnectionPtr & connection)
{
    connection = std::make_unique<Connection>(this->envelope_, pgprocno);
    return ErrorCode::OK;
}

/**
 * @brief constructor of Stub class
 */
Stub::Stub(std::string_view database_name, bool create_shm = false)
    : impl_(std::make_unique<Stub::Impl>(this, database_name, create_shm)) {}

/**
 * @brief connect to the DB and get Connection class.
 */
ErrorCode Stub::get_connection(std::size_t pgprocno, ConnectionPtr & connection)
{    
    return impl_->get_connection(pgprocno, connection);
}

}  // namespace ogawayama::stub
