#pragma once

#include <functional>
#include <ostream>
#include <utility>

#include <cstdint>

#include "date.h"
#include "time_of_day.h"

namespace takatori::datetime {

/**
 * @brief represents time point since the "epoch" as 1970-01-01 00:00:00.
 * @details This epoch depends on the kind of time which the object represents.
 *      For example, if the object represents a local time, the epoch is 1970-01-01 00:00:00 of its local time.
 *      Otherwise, if the object represents a zoned time, the epoch is 1970-01-01 00:00:00 of the **GMT**.
 */
class time_point {
public:
    /// @brief the second offset type.
    using offset_type = std::chrono::duration<std::int64_t>;

    /// @brief the nano-second adjustment type.
    using subsecond_unit = std::chrono::duration<std::uint32_t, std::nano>;

    /// @brief time offset type.
    using difference_type = std::chrono::nanoseconds;

    /**
     * @brief creates a new instance of the epoch time, which represents 1970-01-01 00:00:00.
     */
    constexpr time_point() noexcept = default;

    /**
     * @brief creates a new instance
     * @param offset seconds since 1970-01-01 00:00:00, ignoring leap seconds
     * @param adjustment the sub-second value
     */
    explicit constexpr time_point(offset_type offset, difference_type adjustment = {}) noexcept :
            seconds_offset_ {
                offset + std::chrono::floor<offset_type>(adjustment)
            },
            subsecond_ {
                std::chrono::duration_cast<std::chrono::nanoseconds>(offset - seconds_offset_) + adjustment
            }
    {}

    /**
     * @brief creates a new instance
     * @param offset elapsed time since 1970-01-01 00:00:00, ignoring leap seconds
     */
    template<class Rep, class Period>
    explicit constexpr time_point(std::chrono::duration<Rep, Period> offset) noexcept :
            seconds_offset_ { std::chrono::floor<offset_type>(offset) },
            subsecond_ { std::chrono::duration_cast<subsecond_unit>(offset - seconds_offset_) }
    {}


    /**
     * @brief creates a new instance from date and time.
     * @param date date
     * @param time time of the day
     */
    explicit time_point(datetime::date date, datetime::time_of_day time = {}) noexcept;

    /**
     * @brief creates a new instance.
     * @tparam Clock the clock type
     * @tparam Duration the duration type
     * @param time time system time point
     */
    template<class Clock, class Duration>
    explicit time_point(std::chrono::time_point<Clock, Duration> time) :
        time_point { std::chrono::time_point_cast<clock_unit>(time).time_since_epoch() }
    {}

    /**
     * @brief returns the time point of now in system clock.
     * @return time point of now
     */
    [[nodiscard]] static time_point now();

    /**
     * @brief returns the offset since epoch (1970-01-01 00:00:00), ignoring leap seconds.
     * @details the returned value does not contain sub-seconds value, please check nano_adjustments()
     * @return the seconds offset since epoch
     */
    [[nodiscard]] constexpr offset_type seconds_since_epoch() const noexcept {
        return seconds_offset_;
    }

    /**
     * @brief returns the number of nanoseconds, from start of the second.
     * @return the nanosecond within second (`[0, 10^9)`)
     */
    [[nodiscard]] constexpr subsecond_unit subsecond() const noexcept {
        return subsecond_;
    }

    /**
     * @brief returns the date of the time point.
     * @return the date
     */
    [[nodiscard]] datetime::date date() const {
        using date_unit = std::chrono::duration<datetime::date::difference_type, std::ratio<86'400>>;
        auto days = std::chrono::floor<date_unit>(seconds_offset_);
        return datetime::date(days.count());
    }

    /**
     * @brief returns the time in day of the time point.
     * @return the time in day
     */
    [[nodiscard]] constexpr datetime::time_of_day time() const noexcept {
        using date_unit = std::chrono::duration<datetime::date::difference_type, std::ratio<86'400>>;
        auto days = std::chrono::floor<date_unit>(seconds_offset_);
        auto seconds_in_day = seconds_offset_ - std::chrono::duration_cast<offset_type>(days);
        auto nanos_in_day = std::chrono::duration_cast<datetime::time_of_day::time_unit>(seconds_in_day)
                + subsecond_;
        return datetime::time_of_day(nanos_in_day);
    }

    /**
     * @brief returns a pair of date and time of the time point.
     * @return the date and time
     * @see calendar
     */
    [[nodiscard]] std::pair<datetime::date, datetime::time_of_day> date_time() const {
        return std::make_pair(date(), time());
    }

    /**
     * @brief adds offset into this time point.
     * @param offset the offset
     * @return this
     */
    constexpr time_point& operator+=(difference_type offset) noexcept;

    /**
     * @brief subtracts offset from this time point.
     * @param offset the offset
     * @return this
     */
    constexpr time_point& operator-=(difference_type offset) noexcept;

private:
    offset_type seconds_offset_ {};
    subsecond_unit subsecond_ {};

    using clock_unit = std::chrono::nanoseconds;
};

/**
 * @brief adds a time point and offset.
 * @param a the time point
 * @param b the offset
 * @return the computed time point
 */
inline constexpr time_point operator+(time_point a, time_point::difference_type b) noexcept {
    return time_point {
        a.seconds_since_epoch(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(a.subsecond()) + b,
    };
}

/**
 * @brief adds a time point and offset.
 * @param a the time point
 * @param b the offset
 * @return the computed time point
 */
inline constexpr time_point operator+(time_point::difference_type a, time_point b) noexcept {
    return b + a;
}

/**
 * @brief subtracts offset from the time point.
 * @param a the time point
 * @param b the offset
 * @return the computed time point
 */
inline constexpr time_point operator-(time_point a, time_point::difference_type b) noexcept {
    return a + -b;
}

/**
 * @brief returns difference between two time points.
 * @param a the first time point
 * @param b the second time point
 * @return the difference
 */
inline constexpr time_point::difference_type operator-(time_point a, time_point b) noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
            a.seconds_since_epoch() - b.seconds_since_epoch())
            + std::chrono::duration_cast<std::chrono::nanoseconds>(a.subsecond())
            - std::chrono::duration_cast<std::chrono::nanoseconds>(b.subsecond());
}

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
inline constexpr bool operator==(time_point a, time_point b) noexcept {
    return a.seconds_since_epoch() == b.seconds_since_epoch()
        && a.subsecond() == b.subsecond();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, time_point value);

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(time_point a, time_point b) noexcept {
    return !(a == b);
}

constexpr time_point& time_point::operator+=(difference_type offset) noexcept {
    return *this = *this + offset;
}

constexpr time_point& time_point::operator-=(difference_type offset) noexcept {
    return *this = *this - offset;
}

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::time_point.
template<> struct std::hash<takatori::datetime::time_point> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    constexpr std::size_t operator()(takatori::datetime::time_point object) const noexcept {
        return static_cast<std::size_t>(object.seconds_since_epoch().count()) * 257
                + static_cast<std::size_t>(object.subsecond().count());
    }
};
