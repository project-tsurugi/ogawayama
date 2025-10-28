#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include <cstdlib>

#include <takatori/util/enum_set.h>
#include <takatori/util/enum_tag.h>

namespace takatori::value {

/**
 * @brief represents kind of value.
 */
enum class value_kind {
    // built-in values

    /// @brief special value, including nulls.
    unknown,

    /// @brief boolean type.
    boolean,
    /// @brief 4-octets integral number.
    int4,
    /// @brief 8-octets integral number.
    int8,
    /// @brief 4-octets floating point number.
    float4,
    /// @brief 8-octets floating point number.
    float8,
    /// @brief decimal number.
    decimal,
    /// @brief character sequence.
    character,
    /// @brief octet sequence.
    octet,
    /// @brief bit sequence.
    bit,
    /// @brief date type.
    date,
    /// @brief time of day.
    time_of_day,
    /// @brief time point.
    time_point,
    /// @brief date and time interval.
    datetime_interval,

    // complex values

    /// @brief array type.
    array, // FIXME: impl
    /// @brief record type.
    record, // FIXME: impl

    // special values

    /// @brief custom values for compiler or third party extension.
    extension,
};

/// @brief a set of value_kind.
using value_kind_set = util::enum_set<
        value_kind,
        value_kind::unknown,
        value_kind::extension>;

/**
 * @brief provides implementation type for the value_kind.
 * @tparam Kind the expression kind
 */
template<value_kind Kind> struct type_of {};

/// @copydoc type_of
template<value_kind Kind> using type_of_t = typename type_of<Kind>::type;

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(value_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = value_kind;
    switch (value) {
        case kind::boolean: return "bool"sv;
        case kind::int4: return "int4"sv;
        case kind::int8: return "int8"sv;
        case kind::float4: return "float4"sv;
        case kind::float8: return "float8"sv;
        case kind::decimal: return "decimal"sv;
        case kind::character: return "character"sv;
        case kind::octet: return "octet"sv;
        case kind::bit: return "bit"sv;
        case kind::date: return "date"sv;
        case kind::time_of_day: return "time_of_day"sv;
        case kind::time_point: return "time_point"sv;
        case kind::datetime_interval: return "time_interval"sv;
        case kind::array: return "array"sv;
        case kind::record: return "record"sv;
        case kind::unknown: return "unknown"sv;
        case kind::extension: return "extension"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, value_kind value) {
    return out << to_string_view(value);
}

} // namespace takatori::value
