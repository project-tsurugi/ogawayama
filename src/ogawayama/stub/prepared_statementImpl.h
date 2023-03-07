/*
 * Copyright 2019-2023 tsurugi project.
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

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/common/parameter_set.h"
#include "transactionImpl.h"
#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of PreparedStatement::Impl class
 */
class PreparedStatement::Impl
{
public:
    Impl(PreparedStatement *prepared_statement, std::size_t id)
        : envelope_(prepared_statement), id_(id) {}
    ~Impl() = default;

    auto get_id() { return id_; }

private:
    PreparedStatement *envelope_;

    std::size_t id_;
};

}  // namespace ogawayama::stub
