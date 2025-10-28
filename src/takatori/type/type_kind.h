#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include <cstdlib>

#include <takatori/util/enum_set.h>
#include <takatori/util/enum_tag.h>

namespace takatori::type {

/**
 * @brief represents kind of data type.
 */
enum class type_kind {
    // built-in types

    /// @brief boolean type.
    boolean,
    /// @brief 1-octet integral number type.
    int1,
    /// @brief 2-octets integral number type.
    int2,
    /// @brief 4-octets integral number type.
    int4,
    /// @brief 8-octets integral number type.
    int8,
    /// @brief 4-octets floating point number type.
    float4,
    /// @brief 8-octets floating point number type.
    float8,
    /// @brief decimal number type.
    decimal,
    /// @brief character sequence type.
    character,
    /// @brief octet sequence type.
    octet,
    /// @brief bit sequence type.
    bit,
    /// @brief date type.
    date,
    /// @brief time of day type.
    time_of_day,
    /// @brief time point type.
    time_point,
    /// @brief date-time interval type.
    datetime_interval,

    // large objects
    /// @brief binary large object type.
    blob,
    /// @brief character large object type.
    clob,

    // complex types

    /// @brief array type.
    array, // FIXME: impl
    /// @brief record type.
    record, // FIXME: impl

    // special types
    /// @brief unknown type, including nulls.
    unknown,

    /// @brief reference to table entry.
    row_reference, // FIXME: impl

    /// @brief row ID type.
    row_id,

    /**
     * @brief declared type (a.k.a. user defined type).
     * @details declared type can be described as a combination of non-declared types (usually record type),
     *      but it can change the behavior of type related operations like operator overloading.
     * @see takatori::descriptor::declared_type
     */
    declared,

    /// @brief custom type for compiler or third party extension.
    extension,
};

/// @brief a set of type_kind.
using type_kind_set = util::enum_set<
        type_kind,
        type_kind::boolean,
        type_kind::extension>;

/**
 * @brief provides implementation type for the type_kind.
 * @tparam Kind the expression kind
 */
template<type_kind Kind> struct type_of {};

/// @copydoc type_of
template<type_kind Kind> using type_of_t = typename type_of<Kind>::type;

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(type_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = type_kind;
    switch (value) {
        case kind::boolean: return "bool"sv;
        case kind::int1: return "int1"sv;
        case kind::int2: return "int2"sv;
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
        case kind::datetime_interval: return "datetime_interval"sv;
        case kind::blob: return "blob"sv;
        case kind::clob: return "clob"sv;
        case kind::array: return "array"sv;
        case kind::record: return "record"sv;
        case kind::unknown: return "unknown"sv;
        case kind::row_reference: return "row_reference"sv;
        case kind::row_id: return "row_id"sv;
        case kind::declared: return "declared"sv;
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
inline std::ostream& operator<<(std::ostream& out, type_kind value) {
    return out << to_string_view(value);
}

} // namespace takatori::type
