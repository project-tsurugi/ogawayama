#include <takatori/decimal/triple.h>

#include <limits>

#include <takatori/util/exception.h>
#include <takatori/util/hash.h>

namespace takatori::decimal {

using util::throw_exception;

triple::triple(mpd_uint128_triple_t value) :
    triple {
        value.sign == 0 ? +1 : -1,
        value.hi,
        value.lo,
        static_cast<std::int32_t>(value.exp),
    }
{
    if (value.tag != MPD_TRIPLE_NORMAL) {
        throw_exception(std::invalid_argument("decimal tag must be normal"));
    }
    if (value.exp < std::numeric_limits<std::int32_t>::min()
            || value.exp > std::numeric_limits<std::int32_t>::max()) {
        throw_exception(std::domain_error("exponent value is out of range"));
    }
}

triple::triple(::decimal::Decimal const& value) :
    triple {
        value.as_uint128_triple(),
    }
{}

triple::operator mpd_uint128_triple_t() const noexcept {
    return mpd_uint128_triple_t {
        MPD_TRIPLE_NORMAL,
        sign_ >= 0 ? std::uint8_t { 0 } : std::uint8_t { 1 },
        coefficient_high_,
        coefficient_low_,
        exponent_,
    };
}

triple::operator ::decimal::Decimal() const noexcept {
    return ::decimal::Decimal {
        static_cast<mpd_uint128_triple_t>(*this),
    };
}

triple::triple(char const* text) :
    triple {
        ::decimal::Decimal {
            text,
        }
    }
{}


std::ostream& operator<<(std::ostream& out, triple value) {
    return out << static_cast<::decimal::Decimal>(value);
}

} // namespace takatori::decimal


std::size_t std::hash<::takatori::decimal::triple>::operator()(::takatori::decimal::triple object) const noexcept {
    return ::takatori::util::hash(
            object.sign(),
            object.coefficient_low(),
            object.coefficient_high(),
            object.exponent());
}
