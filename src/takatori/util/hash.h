#pragma once

#include <functional>
#include <type_traits>

#include "detect.h"
#include "reverse.h"

namespace takatori::util {

/**
 * @brief provides whether or not std::hash<T> is supported.
 * @tparam T the value type
 */
template<class T>
using is_hash_support = is_detected<std::hash, T>;

/// @copydoc is_hash_support
template<class T>
constexpr inline bool is_hash_support_v = is_hash_support<T>::value;

/// @brief the prime number for hash code.
inline constexpr std::size_t hash_prime = 31;

/// @cond IMPL_DEFS
namespace impl {

struct hash {
    constexpr std::size_t operator()() const noexcept {
        return 0;
    }

    template<class T>
    constexpr std::size_t operator()(T&& value) const noexcept {
        using h = std::hash<std::remove_const_t<std::remove_reference_t<T>>>;
        return h{}(std::forward<T>(value));
    }

    template<class First, class... Rest>
    constexpr std::size_t operator()(First&& first, Rest&&... rest) const noexcept {
        return operator()(std::forward<First>(first))
            + operator()(std::forward<Rest>(rest)...) * hash_prime;
    }
};

} // namespace impl
/// @endcond

/**
 * @brief returns hash code of the value list
 * @tparam Elements the target element types
 * @param elements the target elements
 * @return the computed hash code
 */
template<class... Elements>
inline constexpr std::enable_if_t<
        (... && is_hash_support_v<Elements>),
        std::size_t>
hash(Elements&&... elements) noexcept {
    return reverse(impl::hash {}, std::forward<Elements>(elements)...);
}

} // namespace takatori::util
