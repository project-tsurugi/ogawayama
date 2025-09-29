#pragma once

#include <chrono>
#include <optional>

#include <cstdint>

namespace takatori::datetime {

/**
 * @brief represents date information.
 */
struct date_info {

    /// @brief the year field of date.
    std::uint32_t year {};

    /// @brief the month field of date.
    std::uint32_t month {};

    /// @brief the day of month field of date.
    std::uint32_t day {};
};

/**
 * @brief represents time of day information.
 */
struct time_info {

    /// @brief the sub-second field type.
    using subsecond_type = std::chrono::duration<std::uint32_t, std::nano>;

    /// @brief the hour field of time of day.
    std::uint32_t hour {};

    /// @brief the minute field of time of day.
    std::uint32_t minute {};

    /// @brief the second field of time of day.
    std::uint32_t second {};

    /// @brief the sub-second field of time of day.
    subsecond_type subsecond {};
};

/**
 * @brief represents timezone offset information.
 */
struct zone_offset_info {

    /// @brief the sign of timezone offset.
    bool plus { true };

    /// @brief the hour field of timezone offset.
    std::uint32_t hour {};

    /// @brief the minute field of timezone offset.
    std::uint32_t minute {};
};

/**
 * @brief represents datetime information.
 */
struct datetime_info {

    /// @brief the date information.
    date_info date;

    /// @brief the time of day information.
    time_info time;

    /// @brief the optional timezone offset information.
    std::optional<zone_offset_info> offset;
};

} // namespace takatori::datetime
