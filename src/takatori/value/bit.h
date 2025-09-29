#pragma once

#include <ostream>
#include <type_traits>

#include <cstdint>

#include <boost/dynamic_bitset.hpp>

#include "value_kind.h"
#include "simple_value.h"

#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief a bit sequence as polymorphic value model.
 */
class bit : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline value_kind tag = value_kind::bit;

    /// @brief the bit block type.
    using block_type = std::uint64_t;

    /// @brief the entity type
    using entity_type = boost::dynamic_bitset<block_type>;

    /// @brief the view type
    using view_type = std::add_lvalue_reference_t<std::add_const_t<entity_type>>;

    /**
     * @brief creates a new instance.
     * @param value the bit string
     */
    explicit bit(entity_type value) noexcept;

    /**
     * @brief creates a new instance.
     * @param bits string representation of bit sequence, each character must be either '0' or '1'
     * @throws std::invalid_argument if the string representation is not valid
     */
    explicit bit(std::string_view bits);

    ~bit() override = default;
    bit(bit const& other) = delete;
    bit& operator=(bit const& other) = delete;
    bit(bit&& other) noexcept = delete;
    bit& operator=(bit&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override;
    [[nodiscard]] bit* clone() const& override;
    [[nodiscard]] bit* clone() && override;

    /**
     * @brief returns the entity value.
     * @return the entity value
     */
    [[nodiscard]] view_type get() const noexcept;

    /// @copydoc get()
    [[nodiscard]] explicit operator view_type() const noexcept;

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    entity_type entity_;
};

/**
 * @brief returns whether or not the two elements are equivalent.
 * @param a the first element
 * @param b the second element
 * @return true if a == b
 * @return false otherwise
 */
bool operator==(bit const& a, bit const& b) noexcept;

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
bool operator!=(bit const& a, bit const& b) noexcept;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, bit const& value);

/**
 * @brief type_of for bit.
 */
template<> struct type_of<bit::tag> : util::meta_type<bit> {};

} // namespace takatori::value

/// @brief provides hash code of takatori::value::bit.
template<> struct std::hash<takatori::value::bit> : hash<takatori::value::data> {};
