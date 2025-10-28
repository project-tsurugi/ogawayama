#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"

#include <takatori/descriptor/declared_type.h>

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief declared type (a.k.a. user defined type).
 */
class declared : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::declared;

    /**
     * @brief creates a new instance.
     * @param binding the type information
     */
    explicit declared(descriptor::declared_type binding) noexcept;

    ~declared() override = default;
    declared(declared const& other) = delete;
    declared& operator=(declared const& other) = delete;
    declared(declared&& other) noexcept = delete;
    declared& operator=(declared&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] declared* clone() const& override;
    [[nodiscard]] declared* clone() && override;

    /**
     * @brief returns the type information of this declared type.
     * @return the type information
     */
    [[nodiscard]] descriptor::declared_type const& binding() const noexcept;

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(declared const& a, declared const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(declared const& a, declared const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, declared const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    descriptor::declared_type binding_;
};

template<> struct type_of<declared::tag> : util::meta_type<declared> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::declared.
template<> struct std::hash<takatori::type::declared> : hash<takatori::type::data> {};
