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
#include <iostream>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <optional>
#include <sys/ioctl.h>
#include <asm/termbits.h>
#include <csignal>

#include <nlohmann/json.hpp>

#include <tateyama/proto/endpoint/request.pb.h>
#include "ogawayama/transport/transport.h"

#include "rsa.h"
#include "authentication.h"

namespace tateyama::authentication {

constexpr static int FORMAT_VERSION = 1;
constexpr static int MAX_EXPIRATION = 365;  // Maximum expiration date that can be specified with tgctl credentials

struct termio* saved{nullptr};  // NOLINT

enum class credential_type {
    not_defined = 0,
    no_auth,
    user_password,
    disabled
};

class credential_helper_class {
public:
    void set_user_password(const std::string& user, const std::string& password) {
        type_ = credential_type::user_password;
        json_text_ = get_json_text(user, password);
    }

    [[nodiscard]] std::string expiration_date() const noexcept {
        return expiration_date_string_;
    }

    void add_credential(tateyama::proto::endpoint::request::ClientInformation& information, const std::function<std::optional<std::string>()>& key_func) {
        switch (type_) {
        case credential_type::disabled:
            break;
        case credential_type::no_auth:
            break;
        case credential_type::user_password:
        {
            auto key_opt = key_func();
            if (key_opt) {
                rsa_encrypter rsa{key_opt.value()};
                std::string c{};
                rsa.encrypt(json_text_, c);
                std::string encrypted_credential = base64_encode(c);
                (information.mutable_credential())->set_encrypted_credential(encrypted_credential);
                break;
            }
            // server authentication is disabled
            break;
        }
        default:
            throw std::runtime_error("no credential specified");
        }
    }

private:
    credential_type type_{credential_type::no_auth};
    std::string json_text_{};
    std::string auth_token_{};
    std::string encrypted_credential_{};

    std::chrono::minutes expiration_{300}; // 5 minutes for connecting a normal session.
    std::string expiration_date_string_{};

    /**
     * @brief maximum length for valid username
     */
    constexpr static std::size_t MAXIMUM_USERNAME_LENGTH = 1024;

    std::string get_json_text(const std::string& user, const std::string& password) {
        nlohmann::json j;
        std::stringstream ss;

        j["format_version"] = FORMAT_VERSION;
        j["user"] = user;
        j["password"] = password;
        if (expiration_.count() > 0) {
            j["expiration_date"] = expiration();
        }
        j["password"] = password;

        ss << j;
        return ss.str();
    }

    std::string& expiration() {
        std::stringstream r;

        const auto t = std::chrono::system_clock::now() + expiration_;
        const auto& ct = std::chrono::system_clock::to_time_t(t);
        const auto gt = std::gmtime(&ct);
        r << std::put_time(gt, "%Y-%m-%dT%H:%M:%S");

        const auto full = std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch());
        const auto sec  = std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch());
        r << "." << std::fixed << std::setprecision(3) << (full.count() - (sec.count() * 1000))
          << "Z";

        expiration_date_string_ = r.str();
        return expiration_date_string_;
    }
};

static credential_helper_class credential_helper{};  // NOLINT

void user_password(const std::string& user_name, const std::string& passowrd) {
    credential_helper.set_user_password(user_name, passowrd);
}

void add_credential(tateyama::proto::endpoint::request::ClientInformation& information, const std::function<std::optional<std::string>()>& key_func) {
    credential_helper.add_credential(information, key_func);
}

}  // tateyama::authentication
