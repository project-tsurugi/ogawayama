#include <takatori/datetime/conversion.h>

#include <string>

#include "parser/parser.h"

namespace takatori::datetime {

using std::string_literals::operator""s; // NOLINT(*-unused-using-decls)

namespace {

std::optional<std::string> validate_date(date_info const& info) {
    if (info.year < 1) {
        return "invalid year field"s;
    }
    if (info.month < 1 || info.month > 12) {
        return "invalid month field"s;
    }
    if (info.day < 1 || info.day > 31) {
        return "invalid day field"s;
    }
    return {};
}

std::optional<std::string> require_date(std::optional<date_info> const& info) {
    if (!info) {
        return "date field is absent"s;
    }
    return validate_date(*info);
}

std::optional<std::string> validate_time(time_info const& info) {
    if (info.hour > 23) {
        return "invalid hour field"s;
    }
    if (info.minute > 59) {
        return "invalid minute field"s;
    }
    if (info.second > 59) {
        return "invalid second field"s;
    }
    if (info.subsecond.count() >= 1'000'000'000) {
        return "invalid sub-second field"s;
    }
    return {};
}

std::optional<std::string> require_time(std::optional<time_info> const& info) {
    if (!info) {
        return "date field is absent"s;
    }
    return validate_time(*info);
}

std::optional<std::string> validate_offset(zone_offset_info const& info) {
    if (info.hour > 23) {
        return "invalid offset hour field"s;
    }
    if (info.minute > 59) {
        return "invalid offset minute field"s;
    }
    return {};
}

std::optional<std::string> require_offset(std::optional<zone_offset_info> const& info) {
    if (!info) {
        return "zone offset field is absent"s;
    }
    return validate_offset(*info);
}

} // namespace

conversion_result<date_info> parse_date(std::string_view contents) {
    parser::parser p {};
    auto result = p(contents);
    if (!result) {
        return std::move(result.error().message);
    }
    auto&& info = result.value();
    if (auto&& error = require_date(info.date)) {
        return std::move(*error);
    }
    if (info.time) {
        return "invalid time field is present"s;
    }
    if (info.offset) {
        return "invalid zone offset field is present"s;
    }
    return *info.date; // NOLINT(bugprone-unchecked-optional-access) - checked in require_date()
}

conversion_result<time_info> parse_time(std::string_view contents) {
    parser::parser p {};
    auto result = p(contents);
    if (!result) {
        return std::move(result.error().message);
    }
    auto&& info = result.value();
    if (info.date) {
        return "invalid date field is present"s;
    }
    if (auto&& error = require_time(info.time)) {
        return std::move(*error);
    }
    if (info.offset) {
        return "invalid zone offset field is present"s;
    }
    return *info.time; // NOLINT(bugprone-unchecked-optional-access) - checked in require_time()
}

conversion_result<datetime_info> parse_datetime(std::string_view contents) {
    parser::parser p {};
    auto result = p(contents);
    if (!result) {
        return std::move(result.error().message);
    }
    auto&& info = result.value();
    if (auto&& error = require_date(info.date)) {
        return std::move(*error);
    }
    if (info.time) {
        if (auto&& error = validate_time(*info.time)) {
            return std::move(*error);
        }
    } else {
        info.time.emplace();
    }
    // NOTE: we don't care whether the offset is empty
    if (info.offset) {
        if (auto&& error = validate_offset(*info.offset)) {
            return std::move(*error);
        }
    }
    return datetime_info { *info.date, *info.time, info.offset }; // NOLINT(bugprone-unchecked-optional-access) - checked in require_date()
}

conversion_result<zone_offset_info> parse_zone_offset(std::string_view contents) {
    parser::parser p {};
    auto result = p(contents);
    if (!result) {
        return std::move(result.error().message);
    }
    auto&& info = result.value();
    if (info.date) {
        return "invalid date field is present"s;
    }
    if (info.time) {
        return "invalid time field is present"s;
    }
    if (auto&& error = require_offset(info.offset)) {
        return std::move(*error);
    }
    return *info.offset; // NOLINT(bugprone-unchecked-optional-access) - checked in require_offset()
}

} // namespace takatori::datetime
