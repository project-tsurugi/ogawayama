#pragma once

#include <ostream>
#include <sstream>
#include <string>

namespace takatori::util {

/**
 * @brief builds character strings using `operator<<`.
 * @details This performs like as std::ostream even if it is in R-value context:
 *      @code
 *      using sb = ::takatori::util::string_builder;
 *      throw std::domain_error(sb {}
 *              << "error at "
 *              << some_value
 *              << sb::to_string);
 *      @endcode
 */
class string_builder {
public:
    /**
     * @brief an operator tag to return the built string.
     * @see to_string
     */
    struct to_string_t {
        /**
         * @brief creates a new instance.
         */
        explicit constexpr to_string_t() = default;
    };

    /// @brief an operator tag to return the built string.
    static constexpr to_string_t to_string {};

    /**
     * @brief creates a new instance.
     */
    string_builder() = default;

    /**
     * @brief appends string representation of the given value.
     * @tparam T the value type
     * @param value the target value
     */
    template<class T>
    void append(T&& value) {
        buffer_ << std::forward<T>(value);
    }

    /**
     * @brief returns the built string.
     * @return the built string
     */
    [[nodiscard]] std::string str() const;

    /**
     * @brief returns the internal buffer.
     * @return the buffer
     */
    [[nodiscard]] std::ostringstream& buffer() noexcept;

    /// @copydoc buffer()
    [[nodiscard]] std::ostringstream const& buffer() const noexcept;

private:
    std::ostringstream buffer_;
};

/**
 * @brief no-op but has similar API with string_builder.
 */
class null_string_builder {
public:
    /// @copydoc string_builder::to_string_t
    using to_string_t = string_builder::to_string_t;

    /// @brief an operator tag to return the built string.
    static constexpr to_string_t to_string {};

    /**
     * @brief creates a new instance.
     */
    constexpr null_string_builder() = default;

    /**
     * @brief no-op.
     * @tparam T the value type
     */
    template<class T>
    constexpr void append(T&&) {
        // no-op
    }

    /**
     * @brief always returns an empty string.
     * @return the built string
     */
    [[nodiscard]] std::string str() const noexcept { // NOLINT: not static for API similarity
        return {};
    }
};

/**
 * @brief a string_builder which builds a meaningful string only if `NDEBUG` is absent.
 */
#if defined(NDEBUG)
using debug_string_builder = null_string_builder;
#else
using debug_string_builder = string_builder;
#endif

/**
 * @brief appends string representation of the given value.
 * @tparam T the value type
 * @param builder the target string builder
 * @param value the target value
 * @return the string builder
 */
template<class T>
string_builder& operator<<(string_builder& builder, T&& value) {
    builder.append(std::forward<T>(value));
    return builder;
}

/// @copydoc operator<<(string_builder&, T&&)
template<class T>
string_builder&& operator<<(string_builder&& builder, T&& value) {
    builder.append(std::forward<T>(value));
    return std::move(builder);
}

/**
 * @brief appends string representation of the given C-style string.
 * @tparam N the array size
 * @param builder the target string builder
 * @param value the target value
 * @return the string builder
 */
template<std::size_t N>
string_builder& operator<<(string_builder& builder, char const (&value)[N]) { // NOLINT
    builder.append(&value[0]);
    return builder;
}

/// @copydoc operator<<(string_builder&, char const(&)[N])
template<std::size_t N>
string_builder&& operator<<(string_builder&& builder, char const (&value)[N]) { // NOLINT
    builder.append(&value[0]);
    return std::move(builder);
}

/**
 * @brief returns the built string.
 * @param builder the target string builder
 * @return the built string
 */
std::string operator<<(string_builder& builder, string_builder::to_string_t);

/// @copydoc operator<<(string_builder&, string_builder::to_string_t)
std::string operator<<(string_builder const& builder, string_builder::to_string_t);

/// @copydoc operator<<(string_builder&, string_builder::to_string_t)
std::string operator<<(string_builder&& builder, string_builder::to_string_t);

/**
 * @brief no-op.
 * @tparam T the value type
 * @return the string builder
 */
template<class T>
constexpr null_string_builder operator<<(null_string_builder, T&&) {
    return {};
}

/**
 * @brief returns an empty string.
 * @return an empty string
 */
inline std::string operator<<(null_string_builder, null_string_builder::to_string_t) {
    return {};
}

} // namespace takatori::util
