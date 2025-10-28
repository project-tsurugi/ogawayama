#pragma once

#include <chrono>
#include <functional>
#include <ostream>

#include <cstdint>

namespace takatori::datetime {

class time_point;

/**
 * @brief represents time of day.
 */
class time_of_day {
public:
    /// @brief time unit.
    using time_unit = std::chrono::duration<std::uint64_t, std::nano>;

    /// @brief unit of second-of-day field.
    using second_unit = std::chrono::duration<std::uint32_t>;

    /// @brief unit of nano-of-second field.
    using subsecond_unit = std::chrono::duration<std::uint32_t, std::nano>;

    /// @brief time offset type.
    using difference_type = std::chrono::nanoseconds;

    /// @brief the modulo of the day in the time unit.
    static constexpr time_unit modulo {
        std::chrono::duration_cast<time_unit>(second_unit { 86'400 } ),
    };

    /// @brief the max value.
    static constexpr time_unit max_value { modulo - time_unit { 1 }, };

    /**
     * @brief creates a new instance which represents 00:00:00 of day.
     */
    constexpr time_of_day() = default;

    /**
     * @brief creates a new instance.
     * @param time_since_epoch the elapsed time since 00:00:00 of day
     */
    explicit constexpr time_of_day(time_unit time_since_epoch) noexcept
        : elapsed_(normalize(time_since_epoch))
    {}

    /**
     * @brief creates a new instance.
     * @param hour hour of day
     * @param minute minute of hour
     * @param second second of minute
     * @param subsecond sub-second value
     */
    constexpr time_of_day(
            std::uint32_t hour,
            std::uint32_t minute,
            std::uint32_t second,
            time_unit subsecond = time_unit::zero()) noexcept:
        time_of_day {
                std::chrono::duration<std::uint64_t, std::chrono::hours::period>(hour)
                        + std::chrono::duration<std::uint64_t, std::chrono::minutes::period>(minute)
                        + std::chrono::duration<std::uint64_t, std::chrono::seconds::period>(second)
                        + subsecond
        }
    {}

    /**
     * @brief returns the elapsed time since 00:00:00 of day.
     * @return the the elapsed time since 00:00:00
     */
    [[nodiscard]] constexpr time_unit time_since_epoch() const noexcept {
        return elapsed_;
    }

    /**
     * @brief returns the hour of day (0-23)
     * @return the hour of day
     */
    [[nodiscard]] constexpr std::uint32_t hour() const noexcept {
        using unit = std::chrono::duration<std::uint64_t, std::chrono::hours::period>;
        return static_cast<std::uint32_t>(std::chrono::duration_cast<unit>(elapsed_).count());
    }

    /**
     * @brief returns the minute of hour (0-59)
     * @return the minute of hour
     */
    [[nodiscard]] constexpr std::uint32_t minute() const noexcept {
        using unit = std::chrono::duration<std::uint64_t, std::chrono::minutes::period>;
        return static_cast<std::uint32_t>(std::chrono::duration_cast<unit>(elapsed_).count() % 60);
    }

    /**
     * @brief returns the second of minute (0-59)
     * @return the second of hour
     */
    [[nodiscard]] constexpr std::uint32_t second() const noexcept {
        using unit = std::chrono::duration<std::uint64_t, std::chrono::seconds::period>;
        return static_cast<std::uint32_t>(std::chrono::duration_cast<unit>(elapsed_).count() % 60);
    }

    /**
     * @brief returns the second-of-day (`[0,86'400)`).
     * @return
     */
    [[nodiscard]] constexpr second_unit second_of_day() const noexcept {
        return std::chrono::floor<second_unit>(elapsed_);
    }

    /**
     * @brief returns the nano-of-second (`[0,10^9)`).
     * @return the sub-second value
     */
    [[nodiscard]] constexpr subsecond_unit subsecond() const noexcept {
        using unit = subsecond_unit;
        return unit { elapsed_.count() % static_cast<unit::rep>(std::nano::den) };
    }

    /**
     * @brief adds offset into this time.
     * @param offset the offset
     * @return this
     */
    constexpr time_of_day& operator+=(difference_type offset) noexcept;

    /**
     * @brief subtracts offset from this time.
     * @param offset the offset
     * @return this
     */
    constexpr time_of_day& operator-=(difference_type offset) noexcept;

private:
    time_unit elapsed_ {};

    static constexpr time_unit normalize(time_unit time) noexcept {
        constexpr std::uint64_t day = 86'400 * static_cast<std::uint64_t>(time_unit::period::den);
        return time_unit(time.count() % day);
    }
};

/**
 * @brief adds a time and offset.
 * @param a the time
 * @param b the offset
 * @return the computed time
 */
inline constexpr time_of_day operator+(time_of_day a, time_of_day::difference_type b) noexcept {
    constexpr auto signed_modulo = std::chrono::duration_cast<time_of_day::difference_type>(time_of_day::modulo);
    auto b_normalized = std::chrono::duration_cast<time_of_day::time_unit>(signed_modulo + b % signed_modulo);
    auto r_normalized = (a.time_since_epoch() + b_normalized) % time_of_day::modulo;
    return time_of_day { r_normalized };
}

/**
 * @brief adds a time and offset.
 * @param a the time
 * @param b the offset
 * @return the computed time
 */
inline constexpr time_of_day operator+(time_of_day::difference_type a, time_of_day b) noexcept {
    return b + a;
}

/**
 * @brief subtracts offset from the time.
 * @param a the time
 * @param b the offset
 * @return the computed time
 */
inline constexpr time_of_day operator-(time_of_day a, time_of_day::difference_type b) noexcept {
    return a + -b;
}

/**
 * @brief returns difference between two times.
 * @param a the first time
 * @param b the second time
 * @return the difference
 */
inline constexpr time_of_day::difference_type operator-(time_of_day a, time_of_day b) noexcept {
    auto sa = std::chrono::duration_cast<time_of_day::difference_type>(a.time_since_epoch());
    auto sb = std::chrono::duration_cast<time_of_day::difference_type>(b.time_since_epoch());
    return sa - sb;
}

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
inline constexpr bool operator==(time_of_day a, time_of_day b) noexcept {
    return a.time_since_epoch() == b.time_since_epoch();
}

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(time_of_day a, time_of_day b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, time_of_day value);

inline constexpr time_of_day& time_of_day::operator+=(difference_type offset) noexcept {
    return *this = *this + offset;
}

inline constexpr time_of_day& time_of_day::operator-=(difference_type offset) noexcept {
    return *this = *this - offset;
}

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::time_of_day.
template<> struct std::hash<takatori::datetime::time_of_day> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    constexpr std::size_t operator()(takatori::datetime::time_of_day object) const noexcept {
        return object.time_since_epoch().count();
    }
};
