#pragma once

#include <functional>
#include <ostream>
#include <type_traits>

namespace takatori::util {

/**
 * @brief a tag that represents "default".
 * @tparam E the enum type
 */
template<class E>
struct enum_default_tag_t {};

/**
 * @brief a tag class for scoped enum values.
 * @tparam E the enum type
 * @tparam Kind the enum value
 */
template<auto Kind>
struct enum_tag_t : enum_default_tag_t<decltype(Kind)> {

    /// @brief the enum type.
    using type = decltype(Kind);

    /// @brief the enum value.
    static inline constexpr type value = Kind;

    /**
     * @brief creates a new object.
     */
    explicit enum_tag_t() = default;

    /**
     * @brief returns the enum value.
     * @return the enum value
     */
    constexpr operator type() const noexcept { return value; } // NOLINT
};

/**
 * @brief a tag object for scoped enum values.
 * @tparam Kind the enum value
 */
template<auto Kind>
inline constexpr enum_tag_t<Kind> enum_tag {};

/**
 * @brief appends string representation of the given tag's value.
 * @tparam Kind the enum value
 * @param out the target output
 * @param tag the target tag
 * @return the output
 */
template<auto Kind>
inline std::ostream& operator<<(std::ostream& out, enum_tag_t<Kind> tag) {
    (void) tag;
    return out << Kind;
}

/**
 * @brief invokes the callback object with an enum_tag argument.
 * @details This invokes Callback::operator()(enum_tag<Kind> Args...).
 * @tparam Kind the enumeration value
 * @tparam Callback the callback object type
 * @tparam Args the callback extra parameter type
 * @param callback the callback object
 * @param args the callback extra arguments
 * @return the invocation result
 */
template<auto Kind, class Callback, class... Args>
inline std::enable_if_t<
        std::is_invocable_v<Callback, enum_tag_t<Kind>, Args...>,
        std::invoke_result_t<Callback, enum_tag_t<Kind>, Args...>>
enum_tag_callback(Callback&& callback, Args&&... args) {
    return std::invoke(
            std::forward<Callback>(callback),
            enum_tag<Kind>,
            std::forward<Args>(args)...);
}

} // namespace takatori::util
