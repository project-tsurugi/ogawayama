#include <takatori/datetime/datetime_interval.h>

#include "printing.h"

namespace takatori::datetime {

std::ostream& operator<<(std::ostream& out, datetime_interval value) {
    out << "datetime_interval(";
    print(out, value.date());
    out << " ";
    print(out, value.time());
    out << ")";
    return out;
}

} // namespace takatori::datetime
