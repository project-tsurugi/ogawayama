/*
 * Copyright 2022-2025 Project Tsurugi.
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
#include <functional>
#include <string>
#include <optional>
#include <filesystem>

#include <tateyama/proto/endpoint/request.pb.h>

namespace tateyama::authentication {

void user_password(const std::string& user_name, const std::string& passowrd);  // NOLINT(readability-redundant-declaration) only for readability

void add_credential(tateyama::proto::endpoint::request::ClientInformation&, const std::function<std::optional<std::string>()>&);  // NOLINT(readability-redundant-declaration) only for readability

}  // tateyama::authentication
