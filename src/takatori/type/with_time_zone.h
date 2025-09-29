#pragma once

#include <ostream>

namespace takatori::type {

/**
 * @brief represents the data type has "with_time_zone" attribute.
 */
class with_time_zone_t {
public:
    /**
     * @brief creates a new instance.
     */
    constexpr explicit with_time_zone_t(bool enabled) noexcept : enabled_ { enabled } {}

    /**
     * @brief returns whether or not this attribute is enabled.
     * @return true if this attribute is enabled
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool enabled() const noexcept {
        return enabled_;
    }

    /**
     * @brief flips enable/disable this attribute.
     * @return the flipped attribute
     */
    [[nodiscard]] constexpr with_time_zone_t operator~() const noexcept {
        return with_time_zone_t { !enabled() };
    }

    /// @copydoc enabled()
    [[nodiscard]] constexpr explicit operator bool() const noexcept {
        return enabled();
    }

private:
    bool enabled_ { true };
};

/// @copydoc with_time_zone_t
constexpr inline with_time_zone_t with_time_zone { true };

/**
 * @brief returns whether or not the both are equivalent.
 * @param left the first element
 * @param right the second element
 * @return true if the both are equivalent
 * @return false otherwise
 */
[[nodiscard]] constexpr bool operator==(with_time_zone_t left, with_time_zone_t right) {
    return static_cast<bool>(left) == static_cast<bool>(right);
}

/**
 * @brief returns whether or not the both are different.
 * @param left the first element
 * @param right the second element
 * @return true if the both are different
 * @return false otherwise
 */
[[nodiscard]] constexpr bool operator!=(with_time_zone_t left, with_time_zone_t right) {
    return !(left == right);
}

/**
 * @brief appends string representation of with_time_zone_t.
 * @param out the target output
 * @param value the value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, with_time_zone_t value) {
    if (value) {
        return out << "with_time_zone";
    }
    return out << "without_time_zone";
}

} // namespace takatori::type
