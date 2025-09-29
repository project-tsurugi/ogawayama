#include <takatori/datetime/time_zone.h>

#include <takatori/util/exception.h>

#include "time_zone_impl.h"

namespace takatori::datetime {

using ::takatori::util::throw_exception;

time_zone const time_zone::UTC { impl::utc() }; // NOLINT

static std::shared_ptr<time_zone::impl> from_offset(std::chrono::minutes offset, bool or_throw) {
    using impl = time_zone::impl;
    using namespace std::chrono;
    using namespace std::chrono_literals;
    if (offset == 0min) {
        return impl::utc();
    }
    if (offset <= -24h || offset >= +24h) {
        if (or_throw) throw_exception(std::out_of_range("invalid time zone offset"));
        return {};
    }
    auto abs_offset = offset.count() < 0 ? -offset : offset;
    auto hour_part = duration_cast<hours>(abs_offset);
    auto minute_part = abs_offset - hour_part;

    std::array<char, 64> buffer; // NOLINT
    std::snprintf(buffer.data(), buffer.size(), // NOLINT
                  "GMT%c%02d:%02d",
                  (offset.count() < 0 ? '-' : '+'),
                  static_cast<int>(hour_part.count()),
                  static_cast<int>(minute_part.count()));

    std::unique_ptr<impl::entity_type> entity { impl::entity_type::createTimeZone(buffer.data()) };
    if (*entity == impl::entity_type::getUnknown()) { // NOLINT(readability-implicit-bool-conversion) -- for ICU <= 69
        return {};
    }
    return std::make_shared<impl>(buffer.data(), std::move(entity));
}

static std::shared_ptr<time_zone::impl> from_symbol(std::string_view symbol, bool or_throw) {
    using impl = time_zone::impl;
    using namespace std::string_view_literals;
    if (symbol.empty()) {
        if (or_throw) throw_exception(std::invalid_argument("empty time zone symbol"));
        return {};
    }
    if (symbol == "UTC"sv) {
        return time_zone::impl::utc();
    }
    icu::UnicodeString string { symbol.data(), static_cast<std::int32_t>(symbol.size()) };
    std::unique_ptr<impl::entity_type> entity { impl::entity_type::createTimeZone(string) };
    if (*entity == impl::entity_type::getUnknown()) { // NOLINT(readability-implicit-bool-conversion) -- for ICU <= 69
        if (or_throw) throw_exception(std::invalid_argument(std::string("unknown time zone symbol: ") += symbol));
        return {};
    }
    return std::make_shared<impl>(std::string { symbol }, std::move(entity));
}

time_zone::time_zone()
    : impl_(impl::utc())
{}

time_zone::time_zone(std::chrono::minutes offset)
    : impl_(from_offset(offset, true))
{}

time_zone::time_zone(std::string_view symbol)
    : impl_(from_symbol(symbol, true))
{}

std::string_view time_zone::id() const noexcept {
    return impl_->id();
}

std::string_view time_zone::name() const noexcept {
    return impl_->name();
}

std::chrono::minutes time_zone::offset() const noexcept {
    return impl_->offset();
}

bool operator==(time_zone const& a, time_zone const& b) noexcept {
    return *a.impl_ == *b.impl_;
}

bool operator!=(time_zone const& a, time_zone const& b) noexcept {
    return !(a == b);
}

std::ostream& operator<<(std::ostream& out, time_zone const& value) {
    return out << *value.impl_;
}

time_zone::time_zone(std::shared_ptr<impl> entity) noexcept
    : impl_(std::move(entity))
{}

std::optional<time_zone> find_time_zone(std::string_view symbol) {
    if (auto result = from_symbol(symbol, false)) {
        return time_zone { std::move(result) };
    }
    return {};
}

} // namespace takatori::datetime

std::size_t
std::hash<takatori::datetime::time_zone>::operator()(takatori::datetime::time_zone const& object) const noexcept {
    using namespace takatori::datetime;
    return std::hash<time_zone::impl>{}(time_zone::impl::unwrap(object));
}
