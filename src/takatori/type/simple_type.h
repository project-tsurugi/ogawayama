#pragma once

#include "data.h"
#include "type_kind.h"

#include <takatori/util/downcast.h>

namespace takatori::type {

/**
 * @brief a model template of simple types, which have no type specific information.
 * @tparam Kind the type kind
 */
template<type_kind Kind>
class simple_type : public data {

    // FIXME: restrict kind

public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = Kind;

    /**
     * @brief creates a new object.
     */
    explicit constexpr simple_type() = default;

    ~simple_type() override = default;
    simple_type(simple_type const& other) = delete;
    simple_type& operator=(simple_type const& other) = delete;
    simple_type(simple_type&& other) noexcept = delete;
    simple_type& operator=(simple_type&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override {
        return Kind;
    }

    [[nodiscard]] simple_type* clone() const& override {
        return new simple_type<Kind>(); // NOLINT
    }

    [[nodiscard]] simple_type* clone() && override {
        return new simple_type<Kind>(); // NOLINT
    }

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override {
        return tag == other.kind() && *this == util::unsafe_downcast<simple_type<Kind>>(other);
    }

    [[nodiscard]] std::size_t hash() const noexcept override {
        return 0;
    }

    std::ostream& print_to(std::ostream& out) const override {
        return out << *this;
    }
};

/**
 * @brief returns whether or not the two elements are equivalent.
 * @tparam Kind the type kind
 * @param a the first element
 * @param b the second element
 * @return always true
 */
template<type_kind Kind>
inline constexpr bool operator==(simple_type<Kind> const& a, simple_type<Kind> const& b) noexcept {
    (void) a;
    (void) b;
    return true;
}

/**
 * @brief returns whether or not the two elements are different.
 * @tparam Kind the type kind
 * @param a the first element
 * @param b the second element
 * @return always false
 */
template<type_kind Kind>
inline constexpr bool operator!=(simple_type<Kind> const& a, simple_type<Kind> const& b) noexcept {
    (void) a;
    (void) b;
    return false;
}

/**
 * @brief appends string representation of the given value.
 * @tparam Kind the type kind
 * @param out the target output
 * @param value the target value
 * @return the output
 */
template<type_kind Kind>
inline std::ostream& operator<<(std::ostream& out, simple_type<Kind> const& value) {
    (void) value;
    return out << Kind << "()";
}

} // namespace takatori::type

/**
 * @brief provides hash code of takatori::type::simple_type.
 * @tparam Kind the type kind
 */
template<takatori::type::type_kind Kind>
struct std::hash<takatori::type::simple_type<Kind>> : hash<takatori::type::data> {};
