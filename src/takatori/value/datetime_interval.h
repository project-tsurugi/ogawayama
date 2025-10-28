#pragma once

#include <cstdint>
#include <ostream>

#include "value_kind.h"
#include "simple_value.h"

#include <takatori/datetime/datetime_interval.h>

#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief represents time interval as polymorphic value model.
 */
class datetime_interval : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline value_kind tag = value_kind::datetime_interval;

    /// @brief the entity type
    using entity_type = datetime::datetime_interval;

    /// @brief the view type
    using view_type = entity_type;

    /**
     * @brief creates a new instance.
     * @param value the time interval
     */
    explicit constexpr datetime_interval(entity_type value) noexcept
        : entity_(value)
    {}

    /**
     * @brief creates a new instance.
     * @param date date interval
     * @param time time interval
     */
    explicit constexpr datetime_interval(datetime::date_interval date, datetime::time_interval time) noexcept
        : entity_(date, time)
    {}

    /**
     * @brief creates a new instance.
     * @param year years offset
     * @param month months offset
     * @param day days offset
     * @param hour hours offset
     * @param minute minutes offset
     * @param second seconds offset
     * @param subsecond sub-second offset
     */
    explicit constexpr datetime_interval(
            datetime::date_interval::unit year,
            datetime::date_interval::unit month,
            datetime::date_interval::unit day,
            std::int32_t hour = {},
            std::int32_t minute = {},
            std::int32_t second = {},
            datetime::time_interval::time_unit subsecond = {}) noexcept
        : datetime_interval(
                datetime::date_interval {
                        year,
                        month,
                        day,
                },
                datetime::time_interval {
                        hour,
                        minute,
                        second,
                        subsecond,
                })
    {}

    ~datetime_interval() override = default;
    datetime_interval(datetime_interval const& other) = delete;
    datetime_interval& operator=(datetime_interval const& other) = delete;
    datetime_interval(datetime_interval&& other) noexcept = delete;
    datetime_interval& operator=(datetime_interval&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override;
    [[nodiscard]] datetime_interval* clone() const& override;
    [[nodiscard]] datetime_interval* clone() && override;

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
inline constexpr bool operator==(datetime_interval const& a, datetime_interval const& b) noexcept {
    return a.get() == b.get();
}

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
inline constexpr bool operator!=(datetime_interval const& a, datetime_interval const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, datetime_interval const& value);

/**
 * @brief type_of for time_interval.
 */
template<> struct type_of<datetime_interval::tag> : util::meta_type<datetime_interval> {};

} // namespace takatori::value

/// @brief provides hash code of takatori::value::time_interval.
template<> struct std::hash<takatori::value::datetime_interval> : hash<takatori::value::data> {};
