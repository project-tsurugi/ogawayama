#pragma once

#include <optional>

#include "type_kind.h"
#include "data.h"
#include "with_time_zone.h"

#include <takatori/util/meta_type.h>

namespace takatori::type {

/**
 * @brief time of day type.
 */
class time_of_day : public data {
public:
    /// @brief the kind of this type.
    static constexpr inline type_kind tag = type_kind::time_of_day;

    /**
     * @brief creates a new instance which does not consider time zone offset.
     */
    constexpr time_of_day() noexcept = default;

    /**
     * @brief creates a new instance.
     * @param with_time_zone whether or not this considers time zone offset
     */
    explicit constexpr time_of_day(with_time_zone_t with_time_zone) noexcept :
        with_time_zone_ { with_time_zone }
    {}

    ~time_of_day() override = default;
    time_of_day(time_of_day const& other) = delete;
    time_of_day& operator=(time_of_day const& other) = delete;
    time_of_day(time_of_day&& other) noexcept = delete;
    time_of_day& operator=(time_of_day&& other) noexcept = delete;

    [[nodiscard]] type_kind kind() const noexcept override;
    [[nodiscard]] time_of_day* clone() const& override;
    [[nodiscard]] time_of_day* clone() && override;

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
    friend bool operator==(time_of_day const& a, time_of_day const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(time_of_day const& a, time_of_day const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, time_of_day const& value);

protected:
    [[nodiscard]] bool equals(data const& other) const noexcept override;
    [[nodiscard]] std::size_t hash() const noexcept override;
    std::ostream& print_to(std::ostream& out) const override;

private:
    bool with_time_zone_ { false };
};

template<> struct type_of<time_of_day::tag> : util::meta_type<time_of_day> {};

} // namespace takatori::type

/// @brief provides hash code of takatori::type::time_of_day.
template<> struct std::hash<takatori::type::time_of_day> : hash<takatori::type::data> {};
