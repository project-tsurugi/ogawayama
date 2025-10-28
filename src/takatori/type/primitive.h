#pragma once

#include "type_kind.h"
#include "simple_type.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief unknown type.
 */
using unknown = simple_type<type_kind::unknown>;

/**
 * @brief boolean type.
 */
using boolean = simple_type<type_kind::boolean>;

/**
 * @brief 1-octet integral number type.
 */
using int1 = simple_type<type_kind::int1>;

/**
 * @brief 2-octets integral number type.
 */
using int2 = simple_type<type_kind::int2>;

/**
 * @brief 4-octets integral number type.
 */
using int4 = simple_type<type_kind::int4>;

/**
 * @brief 8-octets integral number type.
 */
using int8 = simple_type<type_kind::int8>;

/**
 * @brief 4-octets binary floating point number type.
 */
using float4 = simple_type<type_kind::float4>;

/**
 * @brief 8-octets binary floating point number type.
 */
using float8 = simple_type<type_kind::float8>;

/**
 * @brief type_of for unknown.
 */
template<> struct type_of<unknown::tag> : util::meta_type<unknown> {};

/**
 * @brief type_of for boolean.
 */
template<> struct type_of<boolean::tag> : util::meta_type<boolean> {};

/**
 * @brief type_of for int1.
 */
template<> struct type_of<int1::tag> : util::meta_type<int1> {};

/**
 * @brief type_of for int2.
 */
template<> struct type_of<int2::tag> : util::meta_type<int2> {};

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

} // namespace takatori::type
