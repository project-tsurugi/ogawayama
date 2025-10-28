#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"
#include "varying.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief a compiler synthesized type for generated row identifiers.
 */
class row_id : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::row_id;

    /// @brief the sequence type.
    using sequence_type = std::size_t;

    row_id() = delete;

    /**
     * @brief creates a new instance.
     * @param sequence the sequence number of the target relation
     */
    explicit constexpr row_id(sequence_type sequence) noexcept :
        sequence_ { sequence }
    {}

    ~row_id() override = default;
    row_id(row_id const& other) = delete;
    row_id& operator=(row_id const& other) = delete;
    row_id(row_id&& other) noexcept = delete;
    row_id& operator=(row_id&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] row_id* clone() const& override;
    [[nodiscard]] row_id* clone() && override;

    /**
     * @brief returns the sequence number of the target relation.
     * The two row_id types with different sequence numbers are distinguished.
     * @return the sequence number
     */
    [[nodiscard]] constexpr sequence_type sequence() const noexcept {
        return sequence_;
    }

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(row_id const& a, row_id const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(row_id const& a, row_id const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, row_id const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    sequence_type sequence_;
};

template<> struct type_of<row_id::tag> : util::meta_type<row_id> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::row_id.
template<> struct std::hash<takatori::type::row_id> : hash<takatori::type::data> {};
