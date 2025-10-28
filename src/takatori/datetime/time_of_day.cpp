#include <takatori/datetime/time_of_day.h>

#include "printing.h"

namespace takatori::datetime {

std::ostream& operator<<(std::ostream& out, time_of_day value) {
    out << "time_of_day(";
    print(out, value);
    out << ")";
    return out;
}

} // namespace takatori::datetime
