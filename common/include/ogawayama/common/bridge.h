/*
 * Copyright 2019-2021 tsurugi project.
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

namespace ogawayama::common {

/**
 * @brief command used in fdw service
 */
enum class command : std::size_t {
    undefined = 0,
    hello,
    begin,
    commit,
    create_table,
    drop_table,
    create_index,
    drop_index,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(command value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
    case command::undefined: return "undefined"sv;
    case command::hello: return "hello"sv;
    case command::begin: return "begin"sv;
    case command::commit: return "commit"sv;
    case command::create_table: return "create_table"sv;
    case command::drop_table: return "drop_table"sv;
    case command::create_index: return "create_index"sv;
    case command::drop_index: return "drop_index"sv;
    default: return "UNDEFINED";
    }
    std::abort();
}

};  // namespace ogawayama::common
