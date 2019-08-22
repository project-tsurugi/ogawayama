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

Stub::Impl::Impl(std::string_view database_name, bool create_shm) : database_name_(database_name) {}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Stub::Impl::get_connection(std::size_t n, Connection * & connection)
{
    connection = connections_.at(n).get();
    return ErrorCode::OK;
}

/**
 * @brief constructor of Stub class
 */
Stub::Stub(std::string_view database_name, bool create_shm = false)
    : stub_(std::make_unique<Stub::Impl>(database_name, create_shm)) {}

}  // namespace ogawayama::stub
