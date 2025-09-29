#include <takatori/datetime/date_interval.h>

#include "printing.h"

namespace takatori::datetime {

std::ostream& operator<<(std::ostream& out, date_interval value) {
    out << "date_interval(";
    print(out, value);
    out << ")";
    return out;
}

} // namespace takatori::datetime
