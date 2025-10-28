#pragma once

#include <chrono>
#include <functional>
#include <ostream>

#include <cstdint>

namespace takatori::datetime {

class time_point;

/**
 * @brief represents date since 1970-01-01 on gregorian calendar.
 */
class date {
public:
    /// @brief date offset type.
    using difference_type = std::int64_t;

    /// @brief the year value type.
    using year_value_type = std::int32_t;

    /// @brief field value type.
    using size_type = std::uint32_t;

    /// @brief the max year.
    static constexpr year_value_type max_year = +999'999'999;

    /// @brief the min year.
    static constexpr year_value_type min_year = -999'999'999;

    /**
     * @brief creates a new instance which represents 1970-01-01.
     */
    constexpr date() = default;

    /**
     * @brief creates a new instance.
     * @param day_offset the elapsed days since 1970-01-01
     * @throws std::out_of_range if date is out of range (`[-999,999,999/01/01,+999,999,999/12/31]`)
     */
    explicit date(difference_type day_offset);

    /**
     * @brief creates a new instance from year. month, and day triple.
     * @param year the year
     * @param month the month number (1-12) of year
     * @param day the day (1-31) of month
     * @throws std::out_of_range if date is out of range (`[-999,999,999/01/01,+999,999,999/12/31]`)
     */
    explicit date(year_value_type year, size_type month, size_type day);

    /**
     * @brief returns the offset days since 1970-01-01.
     * @return the the offset days
     */
    [[nodiscard]] difference_type days_since_epoch() const noexcept;

    /**
     * @brief returns the year.
     * @return the year
     */
    [[nodiscard]] year_value_type year() const noexcept;

    /**
     * @brief returns the month number (1-12) of the year.
     * @return the month number
     */
    [[nodiscard]] size_type month() const noexcept;

    /**
     * @brief returns the day of the month.
     * @return the day of month
     */
    [[nodiscard]] size_type day() const noexcept;

    /**
     * @brief adds offset into this date.
     * @param offset the day offset
     * @return this
     */
    date& operator+=(difference_type offset);

    /**
     * @brief subtracts offset from this date.
     * @param offset the day offset
     * @return this
     */
    date& operator-=(difference_type offset);

private:
    difference_type offset_days_ {};
};

/**
 * @brief adds days with the date.
 * @param a the date
 * @param b the day offset
 * @return the computed date
 */
date operator+(date a, date::difference_type b);

/**
 * @brief adds days with the date.
 * @param a the day offset
 * @param b the date
 * @return the computed date
 */
date operator+(date::difference_type a, date b);

/**
 * @brief subtracts days from the date.
 * @param a the date
 * @param b the day offset
 * @return the computed date
 */
date operator-(date a, date::difference_type b);

/**
 * @brief returns difference between two dates.
 * @param a the first date
 * @param b the second date
 * @return the duration in days
 */
date::difference_type operator-(date a, date b);

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
bool operator==(date a, date b) noexcept;

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
bool operator!=(date a, date b) noexcept;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, date value);

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::date.
template<> struct std::hash<takatori::datetime::date> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    std::size_t operator()(takatori::datetime::date object) const noexcept;
};
