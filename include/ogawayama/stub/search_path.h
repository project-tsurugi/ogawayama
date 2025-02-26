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

#include <jogasaki/proto/sql/response.pb.h>

namespace ogawayama::stub {

class search_path {
public:
    /**
     * @brief returns a list of the schema name
     * @return a std::vector of the schema name
     */
    [[nodiscard]] virtual const std::vector<std::string>& get_schema_names() const = 0;

    search_path() = default;
    virtual ~search_path() = default;

    constexpr search_path(search_path const&) = delete;
    constexpr search_path(search_path&&) = delete;
    search_path& operator = (search_path const&) = delete;
    search_path& operator = (search_path&&) = delete;
};

}  // namespace ogawayama::stub
