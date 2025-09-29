#include <takatori/datetime/time_interval.h>

#include "printing.h"

namespace takatori::datetime {

std::ostream& operator<<(std::ostream& out, time_interval value) {
    out << "time_interval(";
    print(out, value);
    out << ")";
    return out;
}

} // namespace takatori::datetime
