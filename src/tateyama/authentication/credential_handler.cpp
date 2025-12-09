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
#include <stdexcept>

#include <nlohmann/json.hpp>
#include <boost/regex.hpp>

#include <tateyama/proto/endpoint/request.pb.h>

#include "rsa.h"
#include "token_handler.h"
#include "credential_handler.h"

namespace tateyama::authentication {

constexpr static int FORMAT_VERSION = 1;

void credential_handler::set_disabled() {
    type_ = credential_type::disabled;
}    

void credential_handler::set_no_auth() {
    type_ = credential_type::no_auth;
}

void credential_handler::set_user_password(const std::string& user, const std::string& password) {
    type_ = credential_type::user_password;
    json_text_ = get_json_text(user, password);
}

std::string credential_handler::expiration_date() const noexcept {
    return expiration_date_string_;
}

void credential_handler::set_expiration(std::int32_t expiration) noexcept {
    expiration_ = std::chrono::hours(expiration * 24);
}

void credential_handler::add_credential(tateyama::proto::endpoint::request::ClientInformation& information,
                                        const std::function<std::optional<std::string>()>& key_func) {
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
        }
        // The server is operating with the configuration authentication.enabled=false
        break;
    }
    default:
        throw std::runtime_error("no credential specified");
    }
}

std::string credential_handler::get_json_text(const std::string& user, const std::string& password) {
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

std::string& credential_handler::expiration() {
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

}  // tateyama::authentication
