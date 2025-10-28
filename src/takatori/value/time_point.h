#pragma once

#include <chrono>
#include <cstdint>
#include <ostream>

#include "value_kind.h"
#include "simple_value.h"
#include "date.h"
#include "time_of_day.h"

#include <takatori/datetime/time_point.h>

#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief represents time point as polymorphic value model.
 */
class time_point : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline value_kind tag = value_kind::time_point;

    /// @brief the entity type
    using entity_type = datetime::time_point;

    /// @brief the view type
    using view_type = entity_type;

    /**
     * @brief creates a new instance.
     * @param value the time
     */
    explicit constexpr time_point(entity_type value) noexcept
        : entity_(value)
    {}

    /**
     * @brief creates a new instance.
     * @param date date in GMT
     * @param time time of the date
     */
    explicit time_point(datetime::date date, datetime::time_of_day time = {}) noexcept;

    /**
     * @brief creates a new instance from GMT date and time.
     * @param year the year
     * @param month the month number (1-12) of year
     * @param day the day (1-31) of month
     * @param hour hour of day
     * @param minute minute of hour
     * @param second second of minute
     * @param subsecond sub-second value
     */
    explicit time_point(
            std::int32_t year,
            std::uint32_t month,
            std::uint32_t day,
            std::uint32_t hour,
            std::uint32_t minute,
            std::uint32_t second,
            datetime::time_of_day::time_unit subsecond = {});

    /**
     * @brief creates a new instance.
     * @tparam Clock the clock type
     * @tparam Duration the duration type
     * @param time time system time point
     */
    template<class Clock, class Duration>
    explicit time_point(std::chrono::time_point<Clock, Duration> time) :
            entity_ { time }
    {}

    ~time_point() override = default;
    time_point(time_point const& other) = delete;
    time_point& operator=(time_point const& other) = delete;
    time_point(time_point&& other) noexcept = delete;
    time_point& operator=(time_point&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override;
    [[nodiscard]] time_point* clone() const& override;
    [[nodiscard]] time_point* clone() && override;

    /**
     * @brief returns the entity value.
     * @return the entity value
     */
    [[nodiscard]] constexpr view_type get() const noexcept {
        return entity_;
    }

    /// @copydoc get()
    [[nodiscard]] explicit constexpr operator view_type() const noexcept {
        return get();
    }

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    entity_type entity_;
};

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
inline constexpr bool operator==(time_point const& a, time_point const& b) noexcept {
    return a.get() == b.get();
}

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(time_point const& a, time_point const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, time_point const& value);

/**
 * @brief type_of for time_point.
 */
template<> struct type_of<time_point::tag> : util::meta_type<time_point> {};

} // namespace takatori::value

/// @brief provides hash code of takatori::value::time_point.
template<> struct std::hash<takatori::value::time_point> : hash<takatori::value::data> {};
