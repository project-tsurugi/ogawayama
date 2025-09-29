#pragma once

#include <chrono>
#include <functional>
#include <ostream>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

namespace takatori::datetime {

class time_zone;

std::optional<time_zone> find_time_zone(std::string_view symbol);

/**
 * @brief represents a time zone.
 */
class time_zone {
public:
    /// @private
    class impl;

    /**
     * @brief the UTC time-zone.
     */
    static time_zone const UTC;

    /**
     * @brief creates a new object which represents UTC.
     */
    time_zone();

    /**
     * @brief returns a time zone object from the given symbol.
     * @param symbol the time zone symbol
     * @return the corresponded time zone
     * @throws std::invalid_argument if there is no such a time zone
     * @see find_time_zone()
     */
    explicit time_zone(std::string_view symbol);

    /**
     * @brief returns a time zone object from UTC offset time.
     * @param offset offset time with UTC
     * @return the corresponded time zone
     * @throws std::out_of_range if offset is not in (-24h, +24h)
     */
    explicit time_zone(std::chrono::minutes offset);

    /**
     * @brief returns the time zone ID.
     * @return the time zone ID
     */
    [[nodiscard]] std::string_view id() const noexcept;

    /**
     * @brief returns the name of this time zone.
     * @return the time zone name
     */
    [[nodiscard]] std::string_view name() const noexcept;

    /**
     * @brief returns the time zone offset from GMT.
     * @return time GMT offset
     */
    [[nodiscard]] std::chrono::minutes offset() const noexcept;

    // FIXME: DST info

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(time_zone const& a, time_zone const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(time_zone const& a, time_zone const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, time_zone const& value);

private:
    std::shared_ptr<impl> impl_;

    explicit time_zone(std::shared_ptr<impl> entity) noexcept;

    friend class impl;
    friend std::optional<time_zone> find_time_zone(std::string_view symbol);
};

/**
 * @brief returns a time zone object from the given symbol.
 * @param symbol the time zone symbol
 * @return the corresponded time zone
 * @return empty if there is no such a time zone
 */
std::optional<time_zone> find_time_zone(std::string_view symbol);

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::time_zone.
template<> struct std::hash<takatori::datetime::time_zone> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    std::size_t operator()(takatori::datetime::time_zone const& object) const noexcept;
};
