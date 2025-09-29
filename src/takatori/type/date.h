#pragma once

#include "type_kind.h"
#include "simple_type.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief date type.
 */
using date = simple_type<type_kind::date>;

/**
 * @brief type_of for date.
 */
template<> struct type_of<date::tag> : util::meta_type<date> {};

} // namespace takatori::type
