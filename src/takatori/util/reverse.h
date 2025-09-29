#pragma once

#include <functional>
#include <type_traits>
#include <utility>

namespace takatori::util {

/// @private
namespace impl {

/// @private
template<class Func, class... Args>
struct reverse;

/// @private
template<class Func>
struct reverse<Func> {
    template<class... Args>
    static constexpr auto invoke(Func&& func, Args&&... args) {
        return std::invoke(
                std::forward<Func>(func),
                std::forward<Args>(args)...);
    }
};

/// @private
template<class Func, class Next, class... Rest>
struct reverse<Func, Next, Rest...> {
    template<class... Reverted>
    static constexpr auto invoke(Func&& func, Next&& next, Rest&&... rest, Reverted&&... reverseed) {
        return reverse<Func, Rest...>::invoke(
                std::forward<Func>(func),
                std::forward<Rest>(rest)...,
                std::forward<Reverted>(reverseed)...,
                std::forward<Next>(next));
    }
};
} // namespace impl

/**
 * @brief reverses list of arguments and invoke the function with reversed.
 * @tparam Func the function object type
 * @tparam Args the function parameter types in reversed order
 * @param func the functino object
 * @param args the function arguments in reversed order
 * @return the invocation result
 */
template<class Func, class... Args>
constexpr inline auto reverse(Func&& func, Args&&... args) {
    return impl::reverse<Func, Args...>::invoke(
            std::forward<Func>(func),
            std::forward<Args>(args)...);
}

} // namespace takatori::util
