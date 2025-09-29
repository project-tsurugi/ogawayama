#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"
#include "varying.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief character sequence type.
 */
class character : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::character;

    /// @brief the size type.
    using size_type = std::size_t;

    character() = delete;

    /**
     * @brief creates a new instance which represents fixed-length character sequences (a.k.a. CHAR).
     * FIXME: national
     * @param length the number of units in the character sequence
     */
    explicit constexpr character(character::size_type length) noexcept
        : character(~type::varying, length)
    {}

    /**
     * @brief creates a new instance which represents either fixed- or flexible-length character sequences (a.k.a. CHAR or VARCHAR).
     * FIXME: national
     * @param varying the character string length becomes flexible if enabled, otherwise fixed
     * @param length the max number of units in the character sequence,
     *               or empty for flexible size character sequences
     */
    explicit constexpr character(varying_t varying, std::optional<size_type> length = {}) noexcept
        : varying_(varying.enabled())
        , length_(length)
    {}

    ~character() override = default;
    character(character const& other) = delete;
    character& operator=(character const& other) = delete;
    character(character&& other) noexcept = delete;
    character& operator=(character&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] character* clone() const& override;
    [[nodiscard]] character* clone() && override;

    /**
     * @brief returns whether or not this character sequence can vary its length.
     * @return true if this is flexible length character sequence (a.k.a. VARCHAR)
     * @return false if this is fixed length character sequence (a.k.a. CHAR)
     */
    [[nodiscard]] constexpr bool varying() const noexcept {
        return varying_;
    }

    /**
     * @brief returns the max number of character octets in the character sequence.
     * @note if this object represents fixed-length character sequence, the returned value must not be empty.
     * @return the max number of octets
     * @return empty if it is not defined
     * @see is_varying()
     */
    [[nodiscard]] constexpr std::optional<size_type> length() const noexcept {
        return length_;
    }

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(character const& a, character const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(character const& a, character const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, character const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    bool varying_;
    std::optional<size_type> length_;

    // FIXME: national
};

template<> struct type_of<character::tag> : util::meta_type<character> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::character.
template<> struct std::hash<takatori::type::character> : hash<takatori::type::data> {};
