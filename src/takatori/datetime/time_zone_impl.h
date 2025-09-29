#pragma once

#include <takatori/datetime/time_zone.h>

#include <chrono>
#include <memory>
#include <string>

#include <unicode/timezone.h>
#include <unicode/ustring.h>

namespace takatori::datetime {

/**
 * @brief implementation of time_zone.
 */
class time_zone::impl {
public:
    /// @brief the entity type
    using entity_type = icu::TimeZone;

    /**
     * @brief creates a new instance.
     * @param name the time zone name
     * @param entity the time zone entity
     */
    explicit impl(std::string name, std::unique_ptr<entity_type> entity);

    /**
     * @brief returns the UTC time-zone.
     * @return the UTC time-zone
     */
    static std::shared_ptr<impl> utc();

    /**
     * @brief returns the time zone ID.
     * @return the time zone ID
     */
    [[nodiscard]] std::string const& id() const noexcept;

    /**
     * @brief returns the time zone name.
     * @return the time zone name
     */
    [[nodiscard]] std::string const& name() const noexcept;

    /**
     * @brief returns the time zone offset from GMT.
     * @return time GMT offset
     */
    [[nodiscard]] std::chrono::minutes offset() const noexcept;

    /**
     * @brief returns the time zone entity.
     * @return the time zone entity
     */
    [[nodiscard]] entity_type const& entity() const noexcept;

    /**
     * @brief returns whether or not the two elements are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if a == b
     * @return false otherwise
     */
    friend bool operator==(impl const& a, impl const& b) noexcept;

    /**
     * @brief returns whether or not the two elements are different.
     * @param a the first element
     * @param b the second element
     * @return true if a != b
     * @return false otherwise
     */
    friend bool operator!=(impl const& a, impl const& b) noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, impl const& value);

    /**
     * @brief returns the implementation object.
     * @param object public API object
     * @return the corresponded implementation object
     */
    static impl& unwrap(time_zone& object) noexcept;

    /**
     * @brief returns the implementation object.
     * @param object public API object
     * @return the corresponded implementation object
     */
    static impl const& unwrap(time_zone const& object) noexcept;

    /**
     * @brief returns the implementation object.
     * @param object public API object
     * @return the corresponded implementation object
     */
    static std::shared_ptr<impl> share(time_zone& object) noexcept;

private:
    std::string id_ {};
    std::string name_;
    std::unique_ptr<entity_type> entity_;
};

} // namespace takatori::datetime

/// @brief provides hash code of takatori::datetime::time_zone.
template<> struct std::hash<takatori::datetime::time_zone::impl> {
    /**
     * @brief returns hash code of the given object.
     * @param object the target object
     * @return the computed hash code
     */
    std::size_t operator()(takatori::datetime::time_zone::impl const& object) const noexcept;
};
