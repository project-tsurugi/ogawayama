#pragma once

#include <cstdint>

#include "date_interval.h"
#include "time_interval.h"
#include "datetime_interval.h"

/**
 * @brief utility functions about date and time intervals.
 */
namespace takatori::datetime::interval {

/// @brief alias name of date_interval.
using date = date_interval;

/// @brief alias name of time_interval.
using time = time_interval;

/// @brief alias name of datetime_interval.
using datetime = datetime_interval;

/**
 * @brief returns years offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr date_interval years(date_interval::unit offset) noexcept {
    return date_interval { offset, 0, 0, };
}

/**
 * @brief returns months offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr date_interval months(date_interval::unit offset) noexcept {
    return date_interval { 0, offset, 0, };
}

/**
 * @brief returns months offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr date_interval days(date_interval::unit offset) noexcept {
    return date_interval { 0, 0, offset, };
}

/**
 * @brief returns hours offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr time_interval hours(std::int32_t offset) noexcept {
    return time_interval { offset, 0, 0, };
}

/**
 * @brief returns minutes offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr time_interval minutes(std::int32_t offset) noexcept {
    return time_interval { 0, offset, 0, };
}

/**
 * @brief returns seconds offset.
 * @param offset the offset value
 * @return the interval
 */
inline constexpr time_interval seconds(std::int32_t offset) noexcept {
    return time_interval { 0, 0, offset, };
}

} // namespace takatori::datetime::interval
