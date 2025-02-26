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

#include <ogawayama/stub/table_metadata_adapter.h>

namespace ogawayama::stub {

table_metadata_adapter::table_metadata_adapter(::jogasaki::proto::sql::response::DescribeTable::Success proto) :
    proto_(std::move(proto)) {
}

std::optional<std::string> table_metadata_adapter::database_name() const {
    auto& name = proto_.database_name();
    if (!name.empty()) {
        return name;
    }
    return std::nullopt;
}

std::optional<std::string> table_metadata_adapter::schema_name() const {
    auto& name = proto_.schema_name();
    if (!name.empty()) {
        return name;
    }
    return std::nullopt;
}

const std::string& table_metadata_adapter::table_name() const {
    return proto_.table_name();
}

::google::protobuf::RepeatedPtrField<jogasaki::proto::sql::common::Column> table_metadata_adapter::columns() const {
    return proto_.columns();
}

}  // namespace ogawayama::stub
