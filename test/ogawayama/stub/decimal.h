/*
 * Copyright 2018-2023 Project Tsurugi.
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

namespace jogasaki::utils {

static takatori::decimal::triple read_decimal(std::string_view data, std::size_t scale) {
    // extract lower 8-octets of coefficient
    std::uint64_t c_lo{};
    std::uint64_t shift{};
    for (std::size_t offset = 0;
        offset < data.size() && offset < sizeof(std::uint64_t);
        ++offset) {
        auto pos = data.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(data[pos]) };
        c_lo |= octet << shift;
        shift += 8;
    }

    // extract upper 8-octets of coefficient
    std::uint64_t c_hi {};
    shift = 0;
    for (
        std::size_t offset = sizeof(std::uint64_t);
        offset < data.size() && offset < sizeof(std::uint64_t) * 2;
        ++offset) {
        auto pos = data.size() - offset - 1;
        std::uint64_t octet { static_cast<std::uint8_t>(data[pos]) };
        c_hi |= octet << shift;
        shift += 8;
    }

    bool negative = (static_cast<std::uint8_t>(data[0]) & 0x80U) != 0;

    if (negative) {
        // sign extension
        if (data.size() < sizeof(std::uint64_t) * 2) {
            auto mask = std::numeric_limits<std::uint64_t>::max(); // 0xfff.....
            if(data.size() < sizeof(std::uint64_t)) {
                std::size_t rest = data.size() * 8U;
                c_lo |= mask << rest;
                c_hi = mask;
            } else {
                std::size_t rest = (data.size() - sizeof(std::uint64_t)) * 8U;
                c_hi |= mask << rest;
            }
        }

        c_lo = ~c_lo + 1;
        c_hi = ~c_hi;
        if (c_lo == 0) {
            c_hi += 1; // carry up
        }
        // if negative, coefficient must not be zero
        BOOST_ASSERT(c_lo != 0 || c_hi != 0); // NOLINT
    }

    return takatori::decimal::triple{
        negative ? -1 : +1,
        c_hi,
        c_lo,
        -static_cast<std::int32_t>(scale),
    };
}

}
