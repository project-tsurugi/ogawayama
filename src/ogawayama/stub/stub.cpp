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

#include <stdexcept>

#include "connectionImpl.h"

#include "stubImpl.h"

namespace ogawayama::stub {

Stub::Impl::Impl(Stub *stub, std::string_view database_name)
    : envelope_(stub), database_name_(database_name), connection_container_(database_name) {}

Stub::Impl::~Impl() {}

/**
 * @brief connect to the DB and get Connection class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Stub::Impl::get_connection(ConnectionPtr& connection, std::size_t pgprocno)
{
    std::string sid{};
    try {
        sid = connection_container_.connect();
    } catch (std::runtime_error &e) {
        return ErrorCode::SERVER_FAILURE;
    }
    auto connection_impl = std::make_unique<Connection::Impl>(this, sid, pgprocno);
    connection = std::make_unique<Connection>(std::move(connection_impl));
    return connection->get_impl()->hello();
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
ErrorCode Stub::get_connection(ConnectionPtr & connection, std::size_t pgprocno)
{    
    return impl_->get_connection(connection, pgprocno);
}

}  // namespace ogawayama::stub


// place make_stub() outside the namespace
ERROR_CODE make_stub(StubPtr &stub, std::string_view name)
{
    try {
        stub = std::make_unique<ogawayama::stub::Stub>(name);
    }
    catch (std::runtime_error &e) {
        std::cerr << e.what() << std::endl;
        return ERROR_CODE::SERVER_FAILURE;
    }
    return ERROR_CODE::OK;
}
