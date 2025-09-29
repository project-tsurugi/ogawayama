#pragma once

#include <ostream>

namespace takatori::type {

/**
 * @brief represents the data type has "national" attribute.
 */
struct national_t {
    /**
     * @brief creates a new instance.
     */
    explicit national_t() = default;
};

/// @copydoc national_t
constexpr inline national_t national;

/**
 * @brief appends string representation of national_t.
 * @param out the target output
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, national_t) {
    return out << "national";
}

} // namespace takatori::type
