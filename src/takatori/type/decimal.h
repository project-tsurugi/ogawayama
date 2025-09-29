#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief decimal number type.
 */
class decimal : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::decimal;

    /// @brief the size type.
    using size_type = std::size_t;

    /**
     * @brief creates a new instance.
     * @param precision the max number of decimal digits in integral and fractional part,
     *                  or empty for flexible size decimals
     * @param scale the number of decimal digits in fractional part,
     *              which must be less than or equal to precision if it is defined,
     *              or empty for floating point numbers
     */
    explicit constexpr decimal(std::optional<size_type> precision = {}, std::optional<size_type> scale = {}) noexcept :
        precision_ { precision },
        scale_ { scale }
    {}

    ~decimal() override = default;
    decimal(decimal const& other) = delete;
    decimal& operator=(decimal const& other) = delete;
    decimal(decimal&& other) noexcept = delete;
    decimal& operator=(decimal&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] decimal* clone() const& override;
    [[nodiscard]] decimal* clone() && override;

    /**
     * @brief returns the max number of decimal digits in integral and fractional part.
     * @return the max number of digits
     * @return empty if it is not defined
     */
    [[nodiscard]] constexpr std::optional<size_type> precision() const noexcept {
        return precision_;
    }

    /**
     * @brief returns the number of digits in the fractional part.
     * @return the number of digits in the fractional part
     */
    [[nodiscard]] constexpr std::optional<size_type> scale() const noexcept {
        return scale_;
    }

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(decimal const& a, decimal const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(decimal const& a, decimal const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, decimal const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    std::optional<size_type> precision_;
    std::optional<size_type> scale_;
};

/**
 * @brief type_of for decimal.
 */
template<> struct type_of<decimal::tag> : util::meta_type<decimal> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::decimal.
template<> struct std::hash<takatori::type::decimal> : hash<takatori::type::data> {};
