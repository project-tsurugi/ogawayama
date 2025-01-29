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

#include <ogawayama/stub/table_metadata.h>

namespace ogawayama::stub {

class table_metadata_adapter : public table_metadata {
public:
    explicit table_metadata_adapter(::jogasaki::proto::sql::response::DescribeTable::Success proto);

    [[nodiscard]] std::optional<std::string> database_name() const override;

    [[nodiscard]] std::optional<std::string> schema_name() const override;

    [[nodiscard]] const std::string& table_name() const override;

    [[nodiscard]] ::google::protobuf::RepeatedPtrField<jogasaki::proto::sql::common::Column> columns() const override;

private:
    ::jogasaki::proto::sql::response::DescribeTable::Success proto_;
};

}  // namespace ogawayama::stub
