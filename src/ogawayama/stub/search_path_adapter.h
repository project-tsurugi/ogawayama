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

#include <ogawayama/stub/search_path.h>

namespace ogawayama::stub {

class search_path_adapter : public search_path {
public:
    explicit search_path_adapter(const ::jogasaki::proto::sql::response::GetSearchPath::Success& response);

    [[nodiscard]] const std::vector<std::string>& get_schema_names() const override;
    
private:
    std::vector<std::string> names_{};
};

}  // namespace ogawayama::stub
