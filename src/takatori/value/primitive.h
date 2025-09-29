#pragma once

#include <cstdint>

#include "value_kind.h"
#include "simple_value.h"
#include "unknown_kind.h"

#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief traits for value_kind::unknown.
 */
template<>
struct simple_value_traits<value_kind::unknown> {
    /// @brief the entity type.
    using entity_type = unknown_kind;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief traits for value_kind::boolean.
 */
template<>
struct simple_value_traits<value_kind::boolean> {
    /// @brief the entity type.
    using entity_type = bool;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief traits for value_kind::int4.
 */
template<>
struct simple_value_traits<value_kind::int4> {
    /// @brief the entity type.
    using entity_type = std::int32_t;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief traits for value_kind::int8.
 */
template<>
struct simple_value_traits<value_kind::int8> {
    /// @brief the entity type.
    using entity_type = std::int64_t;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief traits for value_kind::float4.
 */
template<>
struct simple_value_traits<value_kind::float4> {
    /// @brief the entity type.
    using entity_type = float;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief traits for value_kind::float8.
 */
template<>
struct simple_value_traits<value_kind::float8> {
    /// @brief the entity type.
    using entity_type = double;
    /// @brief the view type.
    using view_type = entity_type;
};

/**
 * @brief unknown value.
 */
using unknown = simple_value<value_kind::unknown>;

/**
 * @brief boolean value as polymorphic value model.
 */
using boolean = simple_value<value_kind::boolean>;

/**
 * @brief 4-octets integral number as polymorphic value model.
 */
using int4 = simple_value<value_kind::int4>;

/**
 * @brief 8-octets integral number as polymorphic value model.
 */
using int8 = simple_value<value_kind::int8>;

/**
 * @brief 4-octets binary floating point number as polymorphic value model.
 */
using float4 = simple_value<value_kind::float4>;

/**
 * @brief 8-octets binary floating point number as polymorphic value model.
 */
using float8 = simple_value<value_kind::float8>;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, boolean const& value);

/**
 * @brief type_of for unknown.
 */
template<> struct type_of<unknown::tag> : util::meta_type<unknown> {};

/**
 * @brief type_of for boolean.
 */
template<> struct type_of<boolean::tag> : util::meta_type<boolean> {};

/**
 * @brief type_of for int4.
 */
template<> struct type_of<int4::tag> : util::meta_type<int4> {};

/**
 * @brief type_of for int8.
 */
template<> struct type_of<int8::tag> : util::meta_type<int8> {};

/**
 * @brief type_of for float4.
 */
template<> struct type_of<float4::tag> : util::meta_type<float4> {};

/**
 * @brief type_of for float8.
 */
template<> struct type_of<float8::tag> : util::meta_type<float8> {};

} // namespace takatori::value
