#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include "value_kind.h"
#include "simple_value.h"

#include <takatori/util/meta_type.h>

namespace takatori::value {

/**
 * @brief a character sequence as polymorphic value model.
 */
class character : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline value_kind tag = value_kind::character;

    /// @brief the entity type
    using entity_type = std::string;

    /// @brief the view type
    using view_type = std::string_view;

    /**
     * @brief creates a new instance.
     * @param value the character string
     */
    explicit character(entity_type value) noexcept;

    /**
     * @brief creates a new instance.
     * @tparam T the character like object
     * @param value the character string
     */
    template<class T>
    explicit character(T const& value) :
        entity_ { value }
    {}

    ~character() override = default;
    character(character const& other) = delete;
    character& operator=(character const& other) = delete;
    character(character&& other) noexcept = delete;
    character& operator=(character&& other) noexcept = delete;

    [[nodiscard]] value_kind kind() const noexcept override;
    [[nodiscard]] character* clone() const& override;
    [[nodiscard]] character* clone() && override;

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
bool operator==(character const& a, character const& b) noexcept;

/**
 * @brief returns whether or not the two elements are different.
 * @param a the first element
 * @param b the second element
 * @return true if a != b
 * @return false otherwise
 */
bool operator!=(character const& a, character const& b) noexcept;

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
std::ostream& operator<<(std::ostream& out, character const& value);

/**
 * @brief truncates the given UTF-8 encoded string to the specified size in bytes.
 * @details
 *      This function ensures that the returned string is a well-formed UTF-8 sequence
 *      only if the input string is also well-formed. It truncates the input string
 *      such that the byte size does not exceed the specified limit, without splitting
 *      multi-byte UTF-8 characters.
 *
 *      If the input is not a well-formed UTF-8 string, the function will truncate it
 *      to a length up to the specified size (may be up to 3 bytes smaller).
 *
 *      This function does not modify the original string and returns a view into the
 *      truncated portion.
 *
 *      If the input string is shorter than the specified size, the function returns
 *      just the original string.
 *
 * @param str the input UTF-8 encoded string.
 * @param size the maximum size in bytes.
 * @return a view of the truncated string, which will be no larger than the specified size.
 */
std::string_view truncate_utf8(std::string_view str, std::size_t size);

/**
 * @brief type_of for character.
 */
template<> struct type_of<character::tag> : util::meta_type<character> {};

} // namespace takatori::value

/// @brief provides hash code of takatori::value::character.
template<> struct std::hash<takatori::value::character> : hash<takatori::value::data> {};
