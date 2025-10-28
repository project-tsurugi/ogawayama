#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief an extension point for type models.
 */
class extension : public data {
public:
    /// @brief the extension ID type.
    using extension_id_type = std::size_t;

    /// @brief the minimum extension ID for third party extensions.
    static constexpr extension_id_type minimum_user_extension_id = 10'000;

    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::extension;

    extension() = default;
    ~extension() override = default;
    extension(extension const& other) = delete;
    extension& operator=(extension const& other) = delete;
    extension(extension&& other) noexcept = delete;
    extension& operator=(extension&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept final;
    [[nodiscard]] extension* clone() const& override = 0;
    [[nodiscard]] extension* clone() && override = 0;

    /**
     * @brief returns the extension ID of this type.
     * @return the extension ID
     */
    [[nodiscard]] virtual extension_id_type extension_id() const noexcept = 0;

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(extension const& a, extension const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(extension const& a, extension const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, extension const& value);

protected:
    /**
     * @brief returns whether or not this extension is equivalent to the target one.
     * @param other the target extension
     * @return true if the both are equivalent
     * @return false otherwise
     */
    [[nodiscard]] virtual bool equals(extension const& other) const noexcept = 0;
    [[nodiscard]] bool equals(data const& other) const noexcept final;
    [[nodiscard]] std::size_t hash() const noexcept override = 0;
    std::ostream& print_to(std::ostream& out) const override = 0;
};

/**
 * @brief type_of for extension.
 */
template<> struct type_of<extension::tag> : util::meta_type<extension> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::extension.
template<> struct std::hash<takatori::type::extension> : hash<takatori::type::data> {};
