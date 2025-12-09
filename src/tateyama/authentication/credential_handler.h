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
#pragma once

#include <functional>
#include <string>
#include <optional>
#include <filesystem>

#include <tateyama/proto/endpoint/request.pb.h>

namespace tateyama::authentication {

class credential_handler {
enum class credential_type {
    no_auth,
    user_password,
    disabled
};

public:
    void set_disabled();
    void set_no_auth();
    void set_user_password(const std::string& user, const std::string& password);
    void set_auth_token(const std::string& auth_token);
    void set_file_credential(const std::filesystem::path& path);
    [[nodiscard]] std::string expiration_date() const noexcept;

    // for tgctl credentials
    void set_expiration(std::int32_t expiration) noexcept;

    std::optional<std::filesystem::path> default_credential_path();

    void add_credential(tateyama::proto::endpoint::request::ClientInformation&, const std::function<std::optional<std::string>()>&_func);

    void auth_options();

private:
    credential_type type_{credential_type::no_auth};
    std::string json_text_{};
    std::string auth_token_{};
    std::string encrypted_credential_{};

    std::chrono::minutes expiration_{300}; // 5 minutes for connecting a normal session.
    std::string expiration_date_string_{};

    std::string get_json_text(const std::string& user, const std::string& password);

    std::string& expiration();

    void set_encrypted_credential(const std::string& encrypted_credential);

    bool check_not_more_than_one();
};

}  // tateyama::authentication
