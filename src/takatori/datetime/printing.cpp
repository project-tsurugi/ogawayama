#include "printing.h"

#include <takatori/util/iomanip_util.h>

namespace takatori::datetime {

void print(std::ostream& out, date value) {
    out << value.year() << "-"
        << value.month() << "-"
        << value.day();
}

void print(std::ostream& out, time_of_day value) {
    util::instant_fill fill { out, '0' };
    out << std::setw(2) << value.hour() << ":"
        << std::setw(2) << value.minute() << ":"
        << std::setw(2) << value.second() << "."
        << std::setw(9) << value.subsecond().count();
}

void print(std::ostream& out, date_interval value) {
    util::instant_fill fill { out, '0' };
    util::instant_showpos showpos { out };
    out << value.year() << '/'
        << value.month() << '/'
        << value.day();
}

void print(std::ostream& out, time_interval value) {
    util::instant_fill fill { out, '0' };
    util::instant_showpos showpos { out };
    out << value.day() << ':'
        << value.hour() << ':'
        << value.minute() << ':'
        << value.second() << '.'
        << std::setw(9) << std::noshowpos << std::abs(value.subsecond().count());
}

} // namespace takatori::datetime
