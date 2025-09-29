#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"
#include "varying.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief octet sequence type.
 */
class octet : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::octet;

    /// @brief the size type.
    using size_type = std::size_t;

    octet() = delete;

    /**
     * @brief creates a new instance which represents fixed-length octet sequences.
     * @param length the number of bits in the sequence
     */
    explicit constexpr octet(size_type length) noexcept
        : octet(~type::varying, length)
    {}

    /**
     * @brief creates a new instance which represents flexible-length octet sequences.
     * @param varying the octet sequence length becomes flexible if enabled, otherwise fixed
     * @param length the max number of bits in the sequence,
     *               or empty for flexible size octet sequences
     */
    explicit constexpr octet(varying_t varying, std::optional<size_type> length = {}) noexcept
        : varying_(varying)
        , length_(length)
    {}

    ~octet() override = default;
    octet(octet const& other) = delete;
    octet& operator=(octet const& other) = delete;
    octet(octet&& other) noexcept = delete;
    octet& operator=(octet&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] octet* clone() const& override;
    [[nodiscard]] octet* clone() && override;

    /**
     * @brief returns whether or not this octet sequence can vary its length.
     * @return true if this is flexible length octet sequence
     * @return false if this is fixed length octet sequence
     */
    [[nodiscard]] constexpr bool varying() const noexcept {
        return varying_;
    }

    /**
     * @brief returns the max number of bits in the sequence.
     * @note if this object represents fixed-length octet sequence, the returned value must not be empty.
     * @return the max number of bits
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
    friend bool operator==(octet const& a, octet const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(octet const& a, octet const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, octet const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    bool varying_;
    std::optional<size_type> length_;
};

template<> struct type_of<octet::tag> : util::meta_type<octet> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::octet.
template<> struct std::hash<takatori::type::octet> : hash<takatori::type::data> {};
