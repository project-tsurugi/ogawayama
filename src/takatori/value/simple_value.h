#pragma once

#include "data.h"
#include "value_kind.h"

#include <takatori/util/comparable_traits.h>
#include <takatori/util/downcast.h>
#include <takatori/util/hash.h>
#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief traits for the simple_value.
 * @details This provides the following properties:
 *   @li entity_type the entity type
 *   @li view_type the view type
 * @tparam Kind the value kind, must be simple type
 */
template<value_kind Kind> struct simple_value_traits;

/**
 * @brief a model template of simple values, which have no value specific information.
 * @tparam Kind the value kind
 * @tparam Entity the entity type
 */
template<value_kind Kind>
class simple_value : public data {
public:
    /// @brief the kind of this value.
    static constexpr inline value_kind tag = Kind;

    /// @brief the entity type
    using entity_type = typename simple_value_traits<Kind>::entity_type;

    /// @brief the view type
    using view_type = typename simple_value_traits<Kind>::view_type;

    static_assert(std::is_copy_constructible_v<entity_type>);

    static_assert(util::is_equal_comparable_v<entity_type, entity_type>);
    static_assert(util::is_less_comparable_v<entity_type, entity_type>);
    static_assert(util::is_hash_support_v<entity_type>);

    /**
     * @brief creates a new object.
     */
    explicit constexpr simple_value(entity_type entity = {}) noexcept
        : entity_(std::move(entity))
    {}

    ~simple_value() override = default;
    simple_value(simple_value const& other) = delete;
    simple_value& operator=(simple_value const& other) = delete;
    simple_value(simple_value&& other) noexcept = delete;
    simple_value& operator=(simple_value&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override {
        return Kind;
    }

    [[nodiscard]] simple_value* clone() const& override {
        return new simple_value<Kind>(entity_); // NOLINT
    }

    [[nodiscard]] simple_value* clone() && override {
        return new simple_value<Kind>(std::move(entity_)); // NOLINT
    }

    /**
     * @brief returns the entity value.
     * @return the entity value
     */
    [[nodiscard]] constexpr view_type get() const noexcept {
        return entity_;
    }

    /// @copydoc get()
    [[nodiscard]] explicit constexpr operator view_type() const noexcept {
        return get();
    }

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override {
        return tag == other.kind() && *this == util::unsafe_downcast<simple_value<Kind>>(other);
    }

    [[nodiscard]] std::size_t hash() const noexcept override {
        return util::hash(entity_);
    }

    std::ostream& print_to(std::ostream& out) const override {
        return out << *this;
    }

private:
    entity_type entity_;
};

/**
 * @brief returns whether or not the two elements are equivalent.
 * @tparam Kind the value kind
 * @param a the first element
 * @param b the second element
 * @return true if the both entities are equivalent
 * @return false otherwise
 */
template<value_kind Kind>
inline constexpr bool
operator==(simple_value<Kind> const& a, simple_value<Kind> const& b) noexcept {
    return a.get() == b.get();
}

/**
 * @brief returns whether or not the two elements are different.
 * @tparam Kind the value kind
 * @param a the first element
 * @param b the second element
 * @return true if the both entities are different
 * @return false otherwise
 */
template<value_kind Kind>
inline constexpr bool
operator!=(simple_value<Kind> const& a, simple_value<Kind> const& b) noexcept {
    return !(a == b);
}

/**
 * @brief appends string representation of the given value.
 * @tparam Kind the value kind
 * @param out the target output
 * @param value the target value
 * @return the output
 */
template<value_kind Kind>
inline std::ostream&
operator<<(std::ostream& out, simple_value<Kind> const& value) {
    return out << Kind << "(" << value.get() << ")";
}

} // namespace takatori::value

/**
 * @brief provides hash code of takatori::value::simple_value.
 * @tparam Kind the value kind
 */
template<takatori::value::value_kind Kind>
struct std::hash<takatori::value::simple_value<Kind>> : hash<takatori::value::data> {};
