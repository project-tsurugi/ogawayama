#pragma once

#include <functional>
#include <ostream>

#include <cstdint>

namespace takatori::datetime {

class datetime_interval;

/**
 * @brief represents date interval.
 * @note This is designed for the solar calendar.
 */
class date_interval {
public:
    /// @private the field unit type.
    using unit = std::int32_t;

    /**
     * @brief creates a new instance about 0-long date interval.
     */
    constexpr date_interval() = default;

    /**
     * @brief creates a new instance.
     * @param days day offset
     */
    explicit constexpr date_interval(unit days) noexcept
        : days_(days)
    {}

    /**
     * @brief creates a new instance.
     * @param year years offset
     * @param month months offset
     * @param day days offset
     */
    constexpr date_interval(unit year, unit month, unit day) noexcept
        : years_(year)
        , months_(month)
        , days_(day)
    {}

    /**
     * @brief returns offset of years.
     * @return offset of years
     */
    [[nodiscard]] constexpr unit year() const noexcept {
        return years_;
    }

    /**
     * @brief returns offset of months.
     * @return offset of months
     */
    [[nodiscard]] constexpr unit month() const noexcept {
        return months_;
    }

    /**
     * @brief returns offset of days.
     * @return offset of days
     */
    [[nodiscard]] constexpr unit day() const noexcept {
        return days_;
    }

    /**
     * @brief returns whether or not this is non-zero interval.
     * @return true if this is non-zero
     * @return false otherwise
     */
    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return years_ != 0
                || months_ != 0
                || days_ != 0;
    }

    /**
     * @brief adds the interval into this.
     * @param value the value
     * @return this
     */
    constexpr date_interval& operator+=(date_interval value) noexcept;

    /**
     * @brief subtracts the interval from this.
     * @param value the value
     * @return this
     */
    constexpr date_interval& operator-=(date_interval value) noexcept;

private:
    unit years_ {};
    unit months_ {};
    unit days_ {};
};

/**
 * @brief returns itself.
 * @param value the value
 * @return the value
 */
inline constexpr date_interval operator+(date_interval value) noexcept {
    return value;
}

/**
 * @brief negates the interval.
 * @param value the value
 * @return the negated value
 */
inline constexpr date_interval operator-(date_interval value) noexcept {
    return date_interval {
            -value.year(),
            -value.month(),
            -value.day(),
    };
}

/**
 * @brief adds the two intervals.
 * @param a the first value
 * @param b the second value
 * @return the sum of two
 */
inline constexpr date_interval operator+(date_interval a, date_interval b) noexcept {
    return date_interval {
            a.year() + b.year(),
            a.month() + b.month(),
            a.day() + b.day(),
    };
}

/**
 * @brief subtracts the second interval from the first one.
 * @param a the first value
 * @param b the second value
 * @return the difference of two
 */
inline constexpr date_interval operator-(date_interval a, date_interval b) noexcept {
    return date_interval {
            a.year() - b.year(),
            a.month() - b.month(),
            a.day() - b.day(),
    };
}

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
inline constexpr bool operator==(date_interval a, date_interval b) noexcept {
    return a.year() == b.year()
        && a.month() == b.month()
        && a.day() == b.day();
}

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(date_interval a, date_interval b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, date_interval value);

inline constexpr date_interval& date_interval::operator+=(date_interval value) noexcept {
    return *this = *this + value;
}

inline constexpr date_interval& date_interval::operator-=(date_interval value) noexcept {
    return *this = *this - value;
}

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::date_interval.
template<> struct std::hash<takatori::datetime::date_interval> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    constexpr std::size_t operator()(takatori::datetime::date_interval object) const noexcept {
        std::size_t result = 0;
        result = result * 31 + static_cast<std::size_t>(object.year());
        result = result * 31 + static_cast<std::size_t>(object.month());
        result = result * 257 + static_cast<std::size_t>(object.day());
        return result;
    }
};
