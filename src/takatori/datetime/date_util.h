#pragma once

#include <array>

#include <cstdint>

namespace takatori::datetime::util {

/// @brief the year value type.
using year_value_type = std::int32_t;

/// @brief field value type.
using field_value_type = std::uint32_t;

/// @brief days difference type.
using difference_type = std::int64_t;

/// @brief the max year.
static constexpr year_value_type max_year = +999'999'999L;

/// @brief the min year.
static constexpr year_value_type min_year = -999'999'999L;

/// @brief the max year.
static constexpr difference_type max_days = +365'241'780'471LL;

/// @brief the min year.
static constexpr difference_type min_days = -365'243'219'162LL;

/**
 * @brief year and day of year.
 */
struct year_and_day {
    year_value_type year;
    field_value_type day_of_year;
};

/**
 * @brief month and day of month.
 */
struct month_and_day {
    field_value_type month;
    field_value_type day_of_month;
};

constexpr difference_type days_year = 365;

constexpr field_value_type days_january = 31;
constexpr field_value_type days_february = days_january + 28;
constexpr field_value_type days_march = days_february + 31;
constexpr field_value_type days_april = days_march + 30;
constexpr field_value_type days_may = days_april + 31;
constexpr field_value_type days_june = days_may + 30;
constexpr field_value_type days_july = days_june + 31;
constexpr field_value_type days_august = days_july + 31;
constexpr field_value_type days_september = days_august + 30;
constexpr field_value_type days_october = days_september + 31;
constexpr field_value_type days_november = days_october + 30;

// constants for compute leap year

// leap year comes every 4 years (leap tick)
/// @brief years in leap tick
constexpr year_value_type years_leap_tick = 4;

/// @brief days in leap tick
constexpr difference_type days_leap_tick =
        days_year * years_leap_tick
        + (years_leap_tick / 4)
        - (years_leap_tick / 100)
        + (years_leap_tick / 400);

// every 100 years, leap year does not come even if it is leap tick (century)
/// @brief years in century
constexpr year_value_type years_century = 100;

/// @brief days in century
constexpr difference_type days_century =
        days_year * years_century
        + (years_century / 4)
        - (years_century / 100)
        + (years_century / 400);

// every 400 years, leap year comes even if it is century (leap cycle)

/// @brief years in leap cycle
constexpr year_value_type years_leap_cycle = 400;

/// @brief days in leap cycle
constexpr difference_type days_leap_cycle = // 146'097
        days_year * years_leap_cycle
        + (years_leap_cycle / 4) // 100
        - (years_leap_cycle / 100) // 4
        + (years_leap_cycle / 400) // 1
        ;

/// @brief year of internal epoch - -999,999,999/01/01: easy to compute leap years
constexpr year_value_type internal_epoch_year = min_year;

/// @brief elapsed days from -999'999'999/01/01 to 1970/01/01
constexpr difference_type public_epoch_day_since_internal =
        // -999'999'999/01/01 to +0001/01/01
        (1 - internal_epoch_year) / years_leap_cycle * days_leap_cycle
        // 0001/01/01 to 1970/01/01
        + 719'162;

/**
 * @brief returns whether or not the target year is leap.
 * @param year the target year
 * @return true if the target year is leap
 * @return false otherwise
 */
constexpr bool is_leap(year_value_type year) {
    if (year % 4 != 0) {
        return false;
    }
    return (year % 100) != 0 || (year % 400) == 0;
}

/**
 * @brief compute days since epoch (1970-01-01) from the the year.
 * @param year the year
 * @return the days since epoch
 */
constexpr std::int64_t to_days_since_epoch(year_value_type year) noexcept {
    year_value_type y = year - internal_epoch_year;
    difference_type days_since_internal_epoch = days_year * y + (y / 4) - (y / 100) + (y / 400);
    return days_since_internal_epoch - public_epoch_day_since_internal;
}

/**
 * @brief compute days since epoch (1970-01-01) from the given date triple.
 * @param year the year
 * @param month the month of year
 * @param day the day of month
 * @return the days since epoch
 */
constexpr std::int64_t to_days_since_epoch(
        year_value_type year,
        field_value_type month,
        field_value_type day) noexcept {
    constexpr std::array<field_value_type, 12> days_begin_of_month {
            0,
            days_january,
            days_february,
            days_march,
            days_april,
            days_may,
            days_june,
            days_july,
            days_august,
            days_september,
            days_october,
            days_november,
    };
    difference_type result = to_days_since_epoch(year);
    result += days_begin_of_month[std::min(month - 1, 11U)]; // NOLINT
    result += day - 1;
    if (month >= 3 && is_leap(year)) {
        result += 1;
    }
    return result;
}

/**
 * @brief extracts year and day from days since epoch - 1970/01/01.
 * @param days the number of elapsed days
 * @return the corresponded year
 */
constexpr year_and_day to_year_and_day(difference_type days) {
    // we use internal epoch 0001/01/01 to simplify to compute leap years
    difference_type days_since_internal_epoch = days + public_epoch_day_since_internal;

    // the number of leap year cycles (400years)
    difference_type cycles = days_since_internal_epoch / days_leap_cycle;
    difference_type cycle_rest = days_since_internal_epoch % days_leap_cycle;

    // the century offset in the current leap year cycle (0-3)
    difference_type cent_in_cycle = cycle_rest / days_century;
    difference_type cent_rest = cycle_rest % days_century;
    cent_rest += days_century * (cent_in_cycle / (years_leap_cycle / years_century));
    cent_in_cycle -= (cent_in_cycle / (years_leap_cycle / years_century));

    // the leap tick offset in the current century (0-24)
    difference_type tick_in_cent = cent_rest / days_leap_tick;
    difference_type tick_rest = cent_rest % days_leap_tick;

    // the year offset since the last leap tick (0-3)
    difference_type year_in_tick = tick_rest / days_year;
    year_in_tick -= (year_in_tick / years_leap_tick);

    // compute the year and day of year
    auto year = static_cast<year_value_type>(
            internal_epoch_year
            + years_leap_cycle * cycles
            + years_century * cent_in_cycle
            + years_leap_tick * tick_in_cent
            + year_in_tick);
    return {
            year,
        static_cast<field_value_type>(days - to_days_since_epoch(year)),
    };
}

/**
 * @brief extract month and day from the day of year.
 * @param year the year number
 * @param day_of_year the number of elapsed days from beginning of the year
 * @return a pair of the month number and day in the month
 */
constexpr month_and_day to_month_and_day(year_value_type year, field_value_type day_of_year) {
    auto d = day_of_year;
    if (d < days_january)   return {  1, d + 1 };
    if (d < days_february)  return {  2, d - days_january + 1 };
    if (is_leap(year)) {
        if (d == days_february) return { 2, 29 };
        --d;
    }
    if (d < days_march)     return {  3, d - days_february + 1 };
    if (d < days_april)     return {  4, d - days_march + 1 };
    if (d < days_may)       return {  5, d - days_april + 1 };
    if (d < days_june)      return {  6, d - days_may + 1 };
    if (d < days_july)      return {  7, d - days_june + 1 };
    if (d < days_august)    return {  8, d - days_july + 1 };
    if (d < days_september) return {  9, d - days_august + 1 };
    if (d < days_october)   return { 10, d - days_september + 1 };
    if (d < days_november)  return { 11, d - days_october + 1 };
    return { 12, d - days_november + 1 };
}

} // namespace takatori::datetime::util
