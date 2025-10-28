#pragma once

#include <bitset>
#include <iterator>
#include <type_traits>

#include <cstdint>

#include <takatori/util/exception.h>
#include <takatori/util/static_bitset.h>

namespace takatori::util::details {

/**
 * @brief enumerates entries in takatori::util::enum_set.
 * @tparam Enum the enum type
 * @tparam First the first enum value (inclusive)
 * @tparam Last the last enum value (inclusive)
 */
template<class Enum, Enum First, Enum Last>
class enum_set_enumerator {
public:
    /// @brief the value type.
    using value_type = Enum;

    /// @private
    using reference = void;

    /// @private
    using pointer = void;

    /// @private
    using difference_type = void;

    /// @brief the iterator category tag.
    using iterator_category = std::forward_iterator_tag;

    /// @brief the required bits of the enum set.
    static constexpr std::size_t number_of_universe = static_cast<std::size_t>(
            static_cast<std::underlying_type_t<Enum>>(Last)
            - static_cast<std::underlying_type_t<Enum>>(First)
            + 1);

    /**
     * @brief creates a new object.
     * @param entries the bit set to iterate
     * @param index the target index in the bit set
     */
    explicit constexpr enum_set_enumerator(static_bitset<number_of_universe> const& entries, std::size_t index) noexcept
        : entries_(entries)
        , index_(internal_adjust_forward(index))
    {}

    /**
     * @brief returns value of the iterator position.
     * @return the value
     */
    [[nodiscard]] constexpr value_type operator*() const {
        if (index_ >= number_of_universe) {
            throw_exception(std::out_of_range("invalid offset"));
        }
        return to_element(index_);
    }

    /**
     * @brief advances this iterator.
     * @return this
     */
    constexpr enum_set_enumerator& operator++() noexcept {
        index_ = internal_adjust_forward(index_ + 1);
        return *this;
    }

    /**
     * @brief advances this iterator and returns the previous location.
     * @return the previous location
     */
    constexpr enum_set_enumerator const operator++(int) noexcept { // NOLINT
        auto copy = *this;
        operator++();
        return copy;
    }

    /**
     * @brief returns whether or not the two iterators point the same position.
     * @param a the first value
     * @param b the second value
     * @return true if a = b
     * @return false otherwise
     */
    friend constexpr bool operator==(enum_set_enumerator const& a, enum_set_enumerator const& b) noexcept {
        return std::addressof(a.entries_) == std::addressof(b.entries_)
            && a.index_ == b.index_;
    }

    /**
     * @brief returns whether or not the two iterators point different positions.
     * @param a the first value
     * @param b the second value
     * @return true if a != b
     * @return false otherwise
     */
    friend constexpr bool operator!=(enum_set_enumerator const& a, enum_set_enumerator const& b) noexcept {
        return !(a == b);
    }

private:
    static_bitset<number_of_universe> const& entries_;
    std::size_t index_;

    [[nodiscard]] constexpr std::size_t internal_adjust_forward(std::size_t index) const noexcept {
        constexpr auto npos = static_bitset<number_of_universe>::npos;
        if (auto found = entries_.find(index); found != npos) {
            return found;
        }
        return npos;
    }

    [[nodiscard]] static constexpr value_type to_element(std::size_t index) noexcept {
        return static_cast<value_type>(static_cast<std::underlying_type_t<Enum>>(First) + index);
    }
};

} // namespace takatori::util::details
