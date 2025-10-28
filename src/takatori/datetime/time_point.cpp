#include <takatori/datetime/time_point.h>

#include "printing.h"

namespace takatori::datetime {

using date_unit = std::chrono::duration<std::uint64_t, std::ratio<86'400>>;

time_point::time_point(datetime::date date, datetime::time_of_day time) noexcept :
        time_point {
            offset_type { std::chrono::duration<std::int64_t, std::ratio<86'400>>(date.days_since_epoch()) }
                + time.second_of_day(),
            time.subsecond(),
        }
{}

time_point time_point::now() {
    return time_point { std::chrono::system_clock::now() };
}

std::ostream& operator<<(std::ostream& out, time_point value) {
    out << "time_point(";
    print(out, value.date());
    out << " ";
    print(out, value.time());
    out << ")";
    return out;
}

} // namespace takatori::datetime
