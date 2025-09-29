#pragma once

#include <ostream>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/date_interval.h>
#include <takatori/datetime/time_interval.h>

namespace takatori::datetime {

void print(std::ostream& out, date value);
void print(std::ostream& out, time_of_day value);

void print(std::ostream& out, date_interval value);
void print(std::ostream& out, time_interval value);

} // namespace takatori::datetime
