#pragma once

#include <cstdint>

#include <array>
#include <ostream>
#include <tuple>

#include <decimal.hh>

namespace takatori::decimal {

/**
 * @brief a triple of decimal value, with 128-bit binary integer coefficient.
 * @details the decimal value is expressed as `sign * coefficient * 10 ^ exponent`.
 * @note This is designed only for data exchange.
 */
class triple {
public:
    /**
     * @brief creates a new instance which represents just zero.
     */
    constexpr triple() noexcept = default;

    /**
     * @brief creates a new instance.
     * @note This normalizes sign and coefficient.
     *
     *      - sign is normalized to one of `-1`, `0`, or `+1`
     *      - coefficient will become `0` if sign is `0`
     *      - sign will become `0` if coefficient is `0`
     *
     * @param sign the sign, < 0 to minus, > 0 to plus, or = 0 to zero
     * @param coefficient_high the upper 64-bits of unsigned coefficient
     * @param coefficient_low the lower 64-bits of unsigned coefficient
     * @param exponent the exponent value of ten
     */
    constexpr triple(
            std::int64_t sign,
            std::uint64_t coefficient_high,
            std::uint64_t coefficient_low,
            std::int32_t exponent) noexcept :
        sign_ { extract_sign(sign, coefficient_high, coefficient_low) },
        coefficient_high_ { sign_ == 0 ? 0ULL : coefficient_high },
        coefficient_low_ { sign_ == 0 ? 0ULL : coefficient_low },
        exponent_ { exponent }
    {}

    /**
     * @brief creates a new instance.
     * @param coefficient the signed coefficient
     * @param exponent the exponent value of ten
     */
    constexpr triple( // NOLINT
            std::int64_t coefficient,
            std::int32_t exponent = 0) noexcept :
        triple {
            coefficient,
            0,
            static_cast<std::uint64_t>(coefficient >= 0 ? coefficient : -coefficient),
                exponent,
        }
    {}

    /**
     * @brief returns the sign.
     * @return -1 if value < 0
     * @return 0 if value = 0
     * @return +1 if value > 0
     */
    [[nodiscard]] constexpr std::int8_t sign() const noexcept {
        return sign_;
    }

    /**
     * @brief returns the upper 64-bits of unsigned coefficient.
     * @return the high bits of unsigned coefficient
     */
    [[nodiscard]] constexpr std::uint64_t coefficient_high() const noexcept {
        return coefficient_high_;
    }

    /**
     * @brief returns the lower 64-bits of unsigned coefficient.
     * @return the low bits of unsigned coefficient
     */
    [[nodiscard]] constexpr std::uint64_t coefficient_low() const noexcept {
        return coefficient_low_;
    }

    /**
     * @brief returns the exponent value of ten.
     * @return the exponent
     */
    [[nodiscard]] constexpr std::int32_t exponent() const noexcept {
        return exponent_;
    }

    /**
     * @brief creates a new instance from other representation.
     * @param value the decimal representation
     * @throws std::invalid_argument if the source value is out of range for this class
     */
    explicit triple(mpd_uint128_triple_t value);

    /**
     * @brief converts this to other representation.
     * @return the decimal representation
     */
    explicit operator mpd_uint128_triple_t() const noexcept;

    /**
     * @brief creates a new instance from other representation.
     * @param value the decimal value
     * @throws std::invalid_argument if the source value is out of range for this class
     */
    explicit triple(::decimal::Decimal const& value);

    /**
     * @brief converts this to other representation.
     * @return the decimal representation
     */
    explicit operator ::decimal::Decimal() const noexcept;

    /**
     * @brief parses C-style string and then creates a new decimal instance.
     * @param text the source text
     * @throws std::invalid_argument if text is malformed
     */
    explicit triple(char const* text); // NOLINT

private:
    std::int8_t sign_ {};
    std::uint64_t coefficient_high_ {};
    std::uint64_t coefficient_low_ {};
    std::int32_t exponent_ {};

    constexpr static std::int8_t extract_sign(std::int64_t value, std::uint64_t high, std::uint64_t low) noexcept {
        if (value == 0 || (high == 0 && low == 0)) {
            return 0;
        }
        if (value < 0) {
            return -1;
        }
        return +1;
    }
};

/**
 * @brief compares whether or not two triples have the same representation.
 * @note
 *      This may return `false` even if the two have equivalent values,
 *      for example, `1E+2 (=100)` and `10E+1 (=100)` are NOT same but equivalent.
 * @param left the first element
 * @param right the second element
 * @return true if the both triples have the same representation
 * @return false otherwise
 */
[[nodiscard]] constexpr bool operator==(triple left, triple right) noexcept {
    return left.sign() == right.sign()
        && left.coefficient_low() == right.coefficient_low()
        && left.coefficient_high() == right.coefficient_high()
        && left.exponent() == right.exponent();
}

/**
 * @brief compares whether or not two triples have different representations.
 * @param left the first element
 * @param right the second element
 * @return true if the both triples have different representations
 * @return false otherwise
 */
[[nodiscard]] constexpr bool operator!=(triple left, triple right) noexcept {
    return !(left == right);
}

/**
 * @brief prints information of the given element into the output stream.
 * @param out the target output stream
 * @param value the target element
 * @return the written output stream
 */
std::ostream& operator<<(std::ostream& out, triple value);

} // namespace takatori::decimal

/// @brief provides hash code of takatori::datetime::date.
template<> struct std::hash<::takatori::decimal::triple> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    std::size_t operator()(::takatori::decimal::triple object) const noexcept;
};
