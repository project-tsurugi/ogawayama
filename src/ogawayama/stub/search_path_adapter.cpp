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

#include <ogawayama/stub/table_list_adapter.h>
#include <ogawayama/stub/search_path_adapter.h>

namespace ogawayama::stub {

search_path_adapter::search_path_adapter(const ::jogasaki::proto::sql::response::GetSearchPath::Success& response) {
    for (auto&& e: response.search_paths()) {
        names_.emplace_back(table_list_adapter::get_table_name(e));
    }
}

const std::vector<std::string>& search_path_adapter::get_schema_names() const {
    return names_;
}

}  // namespace ogawayama::stub
