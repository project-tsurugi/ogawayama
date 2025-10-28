#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include <cstdlib>

#include <takatori/util/enum_tag.h>

namespace takatori::value {

/**
 * @brief represents kind of special value.
 */
enum class unknown_kind {
    /// @brief value is absent.
    null = 0,
    /// @brief value is not a number (NaN)
    not_a_number,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(unknown_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = unknown_kind;
    switch (value) {
        case kind::null: return "null"sv;
        case kind::not_a_number: return "not_a_number"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, unknown_kind value) {
    return out << to_string_view(value);
}

} // namespace takatori::value
