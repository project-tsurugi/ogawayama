#include "time_zone_impl.h"

namespace takatori::datetime {

std::shared_ptr<time_zone::impl> time_zone::impl::utc() {
    static std::shared_ptr<impl> const object = std::make_shared<impl>(
            std::string { "UTC" },
            std::unique_ptr<entity_type> { entity_type::createTimeZone("UTC") }
    );
    return object;
}

time_zone::impl::impl(std::string name, std::unique_ptr<entity_type> entity)
    : name_(std::move(name))
    , entity_(std::move(entity))
{
    icu::UnicodeString lid;
    icu::UnicodeString cid;
    entity_->getID(lid);
    UErrorCode status {};
    entity_type::getCanonicalID(lid, cid, status);
    if (U_SUCCESS(status)) { // NOLINT
        cid.toUTF8String(id_);
    } else {
        lid.toUTF8String(id_);
    }
}

std::string const& time_zone::impl::id() const noexcept {
    return id_;
}

std::string const& time_zone::impl::name() const noexcept {
    return name_;
}

std::chrono::minutes time_zone::impl::offset() const noexcept {
    auto offset = std::chrono::milliseconds(entity_->getRawOffset());
    return std::chrono::duration_cast<std::chrono::minutes>(offset);
}

icu::TimeZone const& time_zone::impl::entity() const noexcept {
    return *entity_;
}

time_zone::impl& time_zone::impl::unwrap(time_zone& object) noexcept {
    return *object.impl_;
}

time_zone::impl const& time_zone::impl::unwrap(time_zone const& object) noexcept {
    return *object.impl_;
}

std::shared_ptr<time_zone::impl> time_zone::impl::share(time_zone& object) noexcept {
    return object.impl_;
}

bool operator==(time_zone::impl const& a, time_zone::impl const& b) noexcept {
    return a.id_ == b.id_;
}

bool operator!=(time_zone::impl const& a, time_zone::impl const& b) noexcept {
    return !(a == b);
}

std::ostream& operator<<(std::ostream& out, time_zone::impl const& value) {
    return out << value.name_;
}

} // namespace takatori::datetime

std::size_t std::hash<takatori::datetime::time_zone::impl>::operator()(
        takatori::datetime::time_zone::impl const& object) const noexcept {
    return std::hash<std::string>{}(object.id());
}
