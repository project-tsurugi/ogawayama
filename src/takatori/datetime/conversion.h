#pragma once

#include <optional>
#include <string_view>

#include <takatori/util/either.h>

#include "conversion_info.h"

namespace takatori::datetime {

/**
 * @brief represents the result of conversion, or error message.
 * @tparam T the type of conversion result
 */
template<class T>
using conversion_result = util::either<std::string, T>;

/**
 * @brief parses the given contents as a date.
 * @details The date format must be `YYYY-MM-DD`.
 * @param contents the source contents
 * @return the parsed date information
 * @return an error message if the conversion was failed
 */
[[nodiscard]] conversion_result<date_info> parse_date(std::string_view contents);

/**
 * @brief parses the given contents as a time of day.
 * @details The date format must be `hh:mm:ss.SSSSSSSSS`.
 * @param contents the source contents
 * @return the parsed time of day information
 * @return an error message if the conversion was failed
 */
[[nodiscard]] conversion_result<time_info> parse_time(std::string_view contents);

/**
 * @brief parses the given contents as a datetime, with or without zone offset.
 * @details The date format must be `YY-MM-DD hh:mm:ss.SSSSSSSSS+HH:MM`.
 * @param contents the source contents
 * @return the parsed datetime information with or without zone offset
 * @return an error message if the conversion was failed
 */
[[nodiscard]] conversion_result<datetime_info> parse_datetime(std::string_view contents);

/**
 * @brief parses the given contents as a zone offset.
 * @details The date format must be `+HH:MM` or just `Z`.
 * @param contents the source contents
 * @return the parsed zone offset
 * @return an error message if the conversion was failed
 */
[[nodiscard]] conversion_result<zone_offset_info> parse_zone_offset(std::string_view contents);

} // namespace takatori::datetime
