#pragma once

#include "type_kind.h"
#include "simple_type.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief time_interval type.
 */
using datetime_interval = simple_type<type_kind::datetime_interval>;

/**
 * @brief type_of for time_interval.
 */
template<> struct type_of<datetime_interval::tag> : util::meta_type<datetime_interval> {};

} // namespace takatori::type
