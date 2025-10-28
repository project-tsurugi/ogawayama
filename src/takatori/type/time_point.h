#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"
#include "with_time_zone.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief time point type.
 */
class time_point : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::time_point;

    /**
     * @brief creates a new instance which does not consider time zone offset.
     */
    constexpr time_point() noexcept = default;

    /**
     * @brief creates a new instance.
     * @param with_time_zone whether or not this considers time zone offset
     */
    explicit constexpr time_point(with_time_zone_t with_time_zone) noexcept :
        with_time_zone_ { with_time_zone }
    {}

    ~time_point() override = default;
    time_point(time_point const& other) = delete;
    time_point& operator=(time_point const& other) = delete;
    time_point(time_point&& other) noexcept = delete;
    time_point& operator=(time_point&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] time_point* clone() const& override;
    [[nodiscard]] time_point* clone() && override;

    /**
     * @brief returns whether or not this type considers time zone offset.
     * @return true if this considers time zone offset
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool with_time_zone() const noexcept {
        return with_time_zone_;
    }

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(time_point const& a, time_point const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(time_point const& a, time_point const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, time_point const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    bool with_time_zone_ { false };
};

template<> struct type_of<time_point::tag> : util::meta_type<time_point> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::time_point.
template<> struct std::hash<takatori::type::time_point> : hash<takatori::type::data> {};
