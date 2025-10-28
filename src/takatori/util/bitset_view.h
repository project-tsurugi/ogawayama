#pragma once

#include "basic_bitset_view.h"

namespace takatori::util {

/**
 * @brief provides a view of packed bitset sequence.
 */
using bitset_view = basic_bitset_view<char>;

/**
 * @brief provides a view of unmodifiable packed bitset without buffer ownership.
 */
using const_bitset_view = basic_bitset_view<char const>;

} // namespace takatori::util
