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
#include <optional>

#include <jogasaki/proto/sql/response.pb.h>

namespace ogawayama::stub {

class table_metadata {
public:
    /**
     * @brief returns the database name where the table defined
     * @return the database name, or empty if it is not set
     * @attention this method is not yet implemented
     */
    [[nodiscard]] virtual std::optional<std::string> database_name() const = 0;

    /**
     * @brief returns the schema name where the table defined
     * @return the schema name, or empty if it is not set
     * @attention this method is not yet implemented
     */
    [[nodiscard]] virtual std::optional<std::string> schema_name() const = 0;

    /**
     * @brief returns simple name of the table
     * @return the simple name
     */
    [[nodiscard]] virtual const std::string& table_name() const = 0;

    /**
     * @brief returns the column information of the relation
     * @return the column descriptor list
     */
    [[nodiscard]] virtual ::google::protobuf::RepeatedPtrField<jogasaki::proto::sql::common::Column> columns() const = 0;

    table_metadata() = default;
    virtual ~table_metadata() = default;

    constexpr table_metadata(table_metadata const&) = delete;
    constexpr table_metadata(table_metadata&&) = delete;
    table_metadata& operator = (table_metadata const&) = delete;
    table_metadata& operator = (table_metadata&&) = delete;
};

}  // namespace ogawayama::stub
