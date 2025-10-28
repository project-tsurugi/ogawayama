#pragma once

namespace takatori::util {

/**
 * @brief a meta-type.
 * @tparam T the type which this indicates
 */
template<class T>
struct meta_type {
    /// @brief the indicating type.
    using type = T;
};

} // namespace takatori::util
