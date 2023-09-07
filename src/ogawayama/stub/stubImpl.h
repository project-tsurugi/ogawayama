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
#pragma once

#include <memory>

#include "tateyama/transport/client_wire.h"

#include "ogawayama/stub/api.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Stub::Impl class
 */
class Stub::Impl
{
public:
    Impl(Stub *, std::string_view);
    ~Impl();
    ErrorCode get_connection(ConnectionPtr&, std::size_t);
    std::string_view get_database_name() { return database_name_; }

private:
    const Stub *envelope_;
    const std::string database_name_;
    tateyama::common::wire::connection_container connection_container_;
};

}  // namespace ogawayama::stub
