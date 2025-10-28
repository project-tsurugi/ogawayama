#pragma once

#include <functional>
#include <ostream>

#include "date_interval.h"
#include "time_interval.h"

#include <takatori/util/hash.h>

namespace takatori::datetime {

/**
 * @brief represents date and time interval.
 */
class datetime_interval {
public:
    /**
     * @brief creates a new instance about 0-long time interval.
     */
    constexpr datetime_interval() = default;

    /**
     * @brief creates a new instance.
     * @param date date interval
     * @param time time interval
     */
    constexpr datetime_interval(date_interval date, time_interval time) noexcept
        : date_(date)
        , time_(time)
    {}

    /**
     * @brief creates a new instance.
     * @param date date interval
     */
    constexpr datetime_interval(date_interval date) noexcept // NOLINT
        : date_(date)
    {}

    /**
     * @brief creates a new instance.
     * @param time time interval
     */
    constexpr datetime_interval(time_interval time) noexcept // NOLINT
        : time_(time)
    {}

    /**
     * @brief returns the date interval.
     * @return the date interval
     */
    [[nodiscard]] constexpr date_interval date() const noexcept {
        return date_;
    }

    /**
     * @brief returns the time interval.
     * @return the time interval
     */
    [[nodiscard]] constexpr time_interval time() const noexcept {
        return time_;
    }

    /**
     * @brief returns whether or not this is non-zero interval.
     * @return true if this is non-zero
     * @return false otherwise
     */
    [[nodiscard]] explicit constexpr operator bool() const noexcept {
        return date_ && time_;
    }

    /**
     * @brief adds the interval into this.
     * @param value the value
     * @return this
     */
    constexpr datetime_interval& operator+=(datetime_interval value) noexcept;

    /**
     * @brief subtracts the interval from this.
     * @param value the value
     * @return this
     */
    constexpr datetime_interval& operator-=(datetime_interval value) noexcept;

private:
    date_interval date_ {};
    time_interval time_ {};
};

/**
 * @brief returns itself.
 * @param value the value
 * @return the value
 */
inline constexpr datetime_interval operator+(datetime_interval value) noexcept {
    return value;
}

/**
 * @brief negates the interval.
 * @param value the value
 * @return the negated value
 */
inline constexpr datetime_interval operator-(datetime_interval value) noexcept {
    return datetime_interval {
            -value.date(),
            -value.time(),
    };
}

/**
 * @brief adds the two intervals.
 * @param a the first value
 * @param b the second value
 * @return the sum of two
 */
inline constexpr datetime_interval operator+(datetime_interval a, datetime_interval b) noexcept {
    return datetime_interval {
            a.date() + b.date(),
            a.time() + b.time(),
    };
}

/**
 * @brief subtracts the second interval from the first one.
 * @param a the first value
 * @param b the second value
 * @return the difference of two
 */
inline constexpr datetime_interval operator-(datetime_interval a, datetime_interval b) noexcept {
    return datetime_interval {
            a.date() - b.date(),
            a.time() - b.time(),
    };
}

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
inline constexpr bool operator==(datetime_interval a, datetime_interval b) noexcept {
    return a.date() == b.date()
        && a.time() == b.time();
}

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(datetime_interval a, datetime_interval b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, datetime_interval value);

inline constexpr datetime_interval& datetime_interval::operator+=(datetime_interval value) noexcept {
    return *this = *this + value;
}

inline constexpr datetime_interval& datetime_interval::operator-=(datetime_interval value) noexcept {
    return *this = *this - value;
}

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::time_interval.
template<> struct std::hash<takatori::datetime::datetime_interval> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    constexpr std::size_t operator()(takatori::datetime::datetime_interval object) const noexcept {
        return takatori::util::hash(object.date(), object.time());
    }
};
