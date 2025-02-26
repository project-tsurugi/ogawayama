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

#include "search_path_adapter.h"
#include <ogawayama/stub/table_list.h>

namespace ogawayama::stub {

class table_list_adapter : public table_list {
public:
    explicit table_list_adapter(::jogasaki::proto::sql::response::ListTables::Success proto);

    [[nodiscard]] std::vector<std::string> get_table_names() const override;

    [[nodiscard]] std::vector<std::string> get_simple_names(search_path& sp) const override;

    [[nodiscard]] static std::string get_table_name(const ::jogasaki::proto::sql::response::Name& n);

private:
    ::jogasaki::proto::sql::response::ListTables::Success proto_;
};

}  // namespace ogawayama::stub
