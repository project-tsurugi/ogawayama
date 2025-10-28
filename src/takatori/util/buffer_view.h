#pragma once

#include <string_view>

#include "basic_buffer_view.h"

namespace takatori::util {

/**
 * @brief provides a view of buffer without buffer ownership.
 * @note this is suitable for an editable view of std::string.
 */
using buffer_view = basic_buffer_view<char>;

/**
 * @brief provides a view of unmodifiable buffer without buffer ownership.
 * @note this is compatible with std::string_view.
 *      And `const_buffer_view::iterator` is as same type as `buffer_view::const_iterator`.
 */
using const_buffer_view = basic_buffer_view<char const>;

static_assert(std::is_same_v<buffer_view::const_iterator, const_buffer_view::iterator>);

} // namespace takatori::util
