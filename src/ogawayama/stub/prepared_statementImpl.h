/*
 * Copyright 2019-2023 Project Tsurugi.
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

#include "transactionImpl.h"
#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of PreparedStatement::Impl class
 */
class PreparedStatement::Impl
{
public:
    Impl(Connection::Impl* manager, std::size_t id, bool has_result_records)
        : manager_(manager), id_(id), has_result_records_(has_result_records) {}

    [[nodiscard]] auto get_id() const { return id_; }

    [[nodiscard]] bool has_result_records() const { return has_result_records_; }

    /**
     * @brief get the object to which this belongs
     * @return stub objext
     */
    auto get_manager() { return manager_; }

private:
    Connection::Impl* manager_;

    std::size_t id_;

    bool has_result_records_;
};

}  // namespace ogawayama::stub
