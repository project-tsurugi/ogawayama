#pragma once

#include "type_kind.h"
#include "simple_type.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief binary large object type.
 */
using blob = simple_type<type_kind::blob>;

/**
 * @brief character large object type.
 */
using clob = simple_type<type_kind::clob>;

/**
 * @brief type_of for blob.
 */
template<> struct type_of<blob::tag> : util::meta_type<blob> {};

/**
 * @brief type_of for clob.
 */
template<> struct type_of<clob::tag> : util::meta_type<clob> {};

} // namespace takatori::type
