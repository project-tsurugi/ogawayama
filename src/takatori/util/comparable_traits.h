#pragma once

#include <functional>
#include <type_traits>

#include "detect.h"

namespace takatori::util {

/// @private
namespace impl {

/// @private
template<class A, class B> using equal_t = decltype(std::declval<A>() == std::declval<B>());

/// @private
template<class A, class B> using not_equal_t = decltype(std::declval<A>() != std::declval<B>());

/// @private
template<class A, class B> using less_t = decltype(std::declval<A>() < std::declval<B>());

/// @private
template<class A, class B> using greater_t = decltype(std::declval<A>() > std::declval<B>());

/// @private
template<class A, class B> using less_equal_t = decltype(std::declval<A>() <= std::declval<B>());

/// @private
template<class A, class B> using greater_equal_t = decltype(std::declval<A>() >= std::declval<B>());

} // namespace impl

/**
 * @brief provides whether or not "==" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_equal_comparable = std::is_convertible<detect_t<impl::equal_t, A, B>, bool>;

/// @copydoc is_equal_comparable
template<class A, class B>
constexpr inline bool is_equal_comparable_v = is_equal_comparable<A, B>::value; // NOLINT(modernize-type-traits)

/**
 * @brief provides whether or not "!=" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_not_equal_comparable = std::is_convertible<detect_t<impl::not_equal_t, A, B>, bool>;

/// @copydoc is_not_equal_comparable
template<class A, class B>
constexpr inline bool is_not_equal_comparable_v = is_not_equal_comparable<A, B>::value; // NOLINT(modernize-type-traits)

/**
 * @brief provides whether or not "<" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_less_comparable = std::is_convertible<detect_t<impl::less_t, A, B>, bool>;

/// @copydoc is_less_comparable
template<class A, class B>
constexpr inline bool is_less_comparable_v = is_less_comparable<A, B>::value; // NOLINT(modernize-type-traits)

/**
 * @brief provides whether or not ">" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_greater_comparable = std::is_convertible<detect_t<impl::greater_t, A, B>, bool>;

/// @copydoc is_greater_comparable
template<class A, class B>
constexpr inline bool is_greater_comparable_v = is_greater_comparable<A, B>::value; // NOLINT(modernize-type-traits)

/**
 * @brief provides whether or not "<=" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_less_equal_comparable = std::is_convertible<detect_t<impl::less_equal_t, A, B>, bool>;

/// @copydoc is_less_equal_comparable
template<class A, class B>
constexpr inline bool is_less_equal_comparable_v = is_less_equal_comparable<A, B>::value; // NOLINT(modernize-type-traits)

/**
 * @brief provides whether or not "<=" operator exists between the given types.
 * @tparam A the first type
 * @tparam B the second type
 */
template<class A, class B>
using is_greater_equal_comparable = std::is_convertible<detect_t<impl::greater_equal_t, A, B>, bool>;

/// @copydoc is_greater_equal_comparable
template<class A, class B>
constexpr inline bool is_greater_equal_comparable_v = is_greater_equal_comparable<A, B>::value; // NOLINT(modernize-type-traits)

} // namespace takatori::util
