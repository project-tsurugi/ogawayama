#include <takatori/datetime/date.h>

#include "date_util.h"
#include "printing.h"

#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

namespace takatori::datetime {

static date::difference_type check_days(date::difference_type value) {
    if (value < util::min_days || value > util::max_days) {
        using ::takatori::util::throw_exception;
        using ::takatori::util::string_builder;
        throw_exception(std::out_of_range(string_builder {}
                << "day offset (" << value << ") is out of range "
                << "[" << util::min_days << ", " << util::max_days << "] "
                << "since 1970-01-01"
                << string_builder::to_string));
    }
    return value;
}

static date::difference_type to_days_checked(date::year_value_type year, date::size_type month, date::size_type day) {
    if (year < util::min_year || year > util::max_year) {
        using ::takatori::util::throw_exception;
        using ::takatori::util::string_builder;
        throw_exception(std::out_of_range(string_builder {}
                << "year (" << year << ") is out of range "
                << "[" << util::min_year << ", " << util::max_year << "] "
                << string_builder::to_string));
    }
    return util::to_days_since_epoch(year, month, day);
}

date::date(difference_type day_offset) :
        offset_days_ { check_days(day_offset) }
{}

date::date(year_value_type year, size_type month, size_type day) :
    date {
            to_days_checked(year, month, day),
    }
{}

date::difference_type date::days_since_epoch() const noexcept {
    return offset_days_;
}

date::year_value_type date::year() const noexcept {
    auto v = util::to_year_and_day(offset_days_);
    return v.year;
}

date::size_type date::month() const noexcept {
    auto yad = util::to_year_and_day(offset_days_);
    auto v = util::to_month_and_day(yad.year, yad.day_of_year);
    return v.month;
}

date::size_type date::day() const noexcept {
    auto yad = util::to_year_and_day(offset_days_);
    auto v = util::to_month_and_day(yad.year, yad.day_of_year);
    return v.day_of_month;
}

date& date::operator+=(date::difference_type offset) {
    return *this = *this + offset;
}

date& date::operator-=(date::difference_type offset) {
    return *this = *this - offset;
}

date operator+(date a, date::difference_type b) {
    return date { a.days_since_epoch() + b };
}

date operator+(date::difference_type a, date b) {
    return b + a;
}

date operator-(date a, date::difference_type b) {
    return a + -b;
}

date::difference_type operator-(date a, date b) {
    return a.days_since_epoch() - b.days_since_epoch();
}

bool operator==(date a, date b) noexcept {
    return a.days_since_epoch() == b.days_since_epoch();
}

bool operator!=(date a, date b) noexcept {
    return !(a == b);
}

std::ostream& operator<<(std::ostream& out, date value) {
    out << "date(";
    print(out, value);
    out << ")";
    return out;
}

} // namespace takatori::datetime

std::size_t std::hash<takatori::datetime::date>::operator()(takatori::datetime::date object) const noexcept {
    return object.days_since_epoch();
}
