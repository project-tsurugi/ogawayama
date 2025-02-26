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

namespace ogawayama::stub {

table_list_adapter::table_list_adapter(::jogasaki::proto::sql::response::ListTables::Success proto) :
    proto_(std::move(proto)) {
}

std::vector<std::string> table_list_adapter::get_table_names() const {
    std::vector<std::string> rv;
    for (auto&& n : proto_.table_path_names()) {
        if (n.identifiers_size() > 0) {
            rv.emplace_back(get_table_name(n));
        }
    }
    return rv;
}

std::vector<std::string> table_list_adapter::get_simple_names(search_path& sp) const {
    std::vector<std::string> rv{};
    for (auto&& n : proto_.table_path_names()) {
        if (n.identifiers_size() > 0) {
            for (auto&& e: sp.get_schema_names()) {
                auto tn = get_table_name(n);
                if (tn.rfind(e, 0) == 0) {  // NOLINT(abseil-string-find-startswith) tn.starts_with(e) in c++20
                    rv.emplace_back(tn);
                }
            }
        }
    }
    return rv;
}

std::string table_list_adapter::get_table_name(const ::jogasaki::proto::sql::response::Name& n) {
    std::stringstream ss{};
    auto size = n.identifiers_size();
    if (size > 0) {
        ss << n.identifiers().Get(0).label();
        for (std::size_t i = 1; i < size; i++) {
            ss << '.' << n.identifiers().Get(static_cast<int>(i)).label();
        }
        return ss.str();
    }
    return {};
}

}  // namespace ogawayama::stub
