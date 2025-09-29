#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include "value_kind.h"
#include "simple_value.h"

#include <takatori/util/either.h>
#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief an octet sequence as polymorphic value model.
 */
class octet : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline value_kind tag = value_kind::octet;

    /// @brief the entity type
    using entity_type = std::string;

    /// @brief the view type
    using view_type = std::string_view;

    /**
     * @brief creates a new instance.
     * @param value the octet string
     */
    explicit octet(entity_type value) noexcept;

    /**
     * @brief creates a new instance.
     * @tparam T the octet string
     * @param value the octet string
     */
    template<class T>
    explicit octet(T const& value) : entity_ { value } {}

    ~octet() override = default;
    octet(octet const& other) = delete;
    octet& operator=(octet const& other) = delete;
    octet(octet&& other) noexcept = delete;
    octet& operator=(octet&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override;
    [[nodiscard]] octet* clone() const& override;
    [[nodiscard]] octet* clone() && override;

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
bool operator==(octet const& a, octet const& b) noexcept;

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
bool operator!=(octet const& a, octet const& b) noexcept;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, octet const& value);

/**
 * @brief parses the given literal as an octet string.
 * @details The input literal must be a sequence of __octet values__, that is consists of two hexadecimal digits.
 * @param literal the source literal
 * @param buffer the buffer to store the parsed octet in its tail
 * @return the number of parsed octets
 * @return an error message if the conversion was failed
 */
[[nodiscard]] util::either<std::string, std::size_t> parse_octet(std::string_view literal, std::string& buffer);

/**
 * @brief type_of for octet.
 */
template<> struct type_of<octet::tag> : util::meta_type<octet> {};

} // namespace takatori::value

/// @brief provides hash code of takatori::value::octet.
template<> struct std::hash<takatori::value::octet> : hash<takatori::value::data> {};
