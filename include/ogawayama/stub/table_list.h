/*
 * Copyright 2019-2025 Project Tsurugi.
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

#include <string>
#include <vector>
#include <sstream>

#include <jogasaki/proto/sql/response.pb.h>

#include "search_path.h"

namespace ogawayama::stub {

class table_list {
public:
    /**
     * @brief returns a list of the available table names in the database, except system tables
     * @return a list of the available table names
     */
    [[nodiscard]] virtual std::vector<std::string> get_table_names() const = 0;

    /**
     * @brief returns the schema name where the table defined
     * @param sp the search path
     * @return a std::vector of simple_names
     */
    [[nodiscard]] virtual std::vector<std::string> get_simple_names(search_path& sp) const = 0;

    table_list() = default;
    virtual ~table_list() = default;

    constexpr table_list(table_list const&) = delete;
    constexpr table_list(table_list&&) = delete;
    table_list& operator = (table_list const&) = delete;
    table_list& operator = (table_list&&) = delete;
};

}  // namespace ogawayama::stub
