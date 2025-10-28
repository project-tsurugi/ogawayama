#pragma once

#include <array>
#include <ostream>
#include <limits>
#include <type_traits>

#include <cstddef>
#include <cstdint>

#include "exception.h"

namespace takatori::util {

/**
 * @brief constexpr fixed sized bitset.
 * @tparam N the number of available bits
 */
template<std::size_t N>
class static_bitset {
public:
    /// @brief the size type.
    using size_type = std::size_t;
    /// @brief the bit block type.
    using block_type = std::conditional_t<
            (N <= 8), std::uint8_t, std::conditional_t<
                    (N <= 16), std::uint16_t, std::conditional_t<
                            (N <= 32), std::uint32_t, std::uint64_t>>>;

    /// @brief the number of available bits.
    static constexpr size_type nbits = N;
    /// @brief the number of available bits in block.
    static constexpr size_type nbits_in_block = std::numeric_limits<block_type>::digits;
    /// @brief the number of blocks.
    static constexpr size_type nblocks = (nbits + nbits_in_block - 1) / nbits_in_block;

    /// @brief represents "no such position."
    static constexpr size_type npos = static_cast<size_type>(-1);

    /**
     * @brief the bit reference type.
     */
    class reference {
    public:
        /**
         * @brief creates a new instance.
         * @param entity the source bitset
         * @param position target position on the bitset
         */
        constexpr explicit reference(static_bitset& entity, static_bitset::size_type position) noexcept
            : entity_(entity)
            , position_(position)
        {}

        ~reference() = default;

        /**
         * @brief creates a new instance.
         * @param other the copy source
         */
        constexpr reference(reference const& other) noexcept = default;

        /**
         * @brief sets the given value into the referred bit.
         * @param other reference of the target value
         * @return this
         */
        reference& operator=(reference const& other) noexcept {
            entity_.set_unsafe(position_, other.get());
            return *this;
        }

        reference(reference&& other) noexcept = delete;
        reference& operator=(reference&& other) noexcept = delete;

        /**
         * @brief returns the referred bit.
         * @return the referred bit
         */
        [[nodiscard]] constexpr bool get() const noexcept {
            return entity_.get(position_);
        }

        /// @copydoc get()
        [[nodiscard]] constexpr operator bool() const noexcept { // NOLINT
            return get();
        }

        /**
         * @brief sets the given value into the referred bit.
         * @param value the value
         * @return this
         */
        constexpr reference& operator=(bool value) noexcept {
            entity_.set_unsafe(position_, value);
            return *this;
        }

        /**
         * @brief sets `false` into referred bit only if the target value is `false`
         * @param value the target value
         * @return this
         */
        constexpr reference& operator&=(bool value) noexcept {
            if (!value) {
                entity_.set_unsafe(position_, false);
            }
            return *this;
        }

        /**
         * @brief sets `true` into referred bit only if the target value is `true`
         * @param value the target value
         * @return this
         */
        constexpr reference& operator|=(bool value) noexcept {
            if (value) {
                entity_.set_unsafe(position_, true);
            }
            return *this;
        }

        /**
         * @brief flips the referred bit only if the target value is `true`
         * @param value the target value
         * @return this
         */
        constexpr reference& operator^=(bool value) noexcept {
            if (value) {
                entity_.flip_unsafe(position_);
            }
            return *this;
        }

    private:
        static_bitset& entity_;
        static_bitset::size_type position_;
    };

    /**
     * @brief creates a new instance which all entries are 0.
     */
    constexpr static_bitset() = default;

    /**
     * @brief creates a new instance from the array of bool.
     * @param entries the bit sequence representation
     */
    constexpr static_bitset(std::array<bool, nbits> entries) noexcept { // NOLINT
        for (size_type i = 0; i < nbits; ++i) {
            set_unsafe(i, entries[i]);
        }
    }

    /**
     * @brief creates a new instance from the list of boolean convertible values.
     * @tparam the value type
     * @param entries the bit sequence representation
     * @throws std::out_of_range if too many elements in the input
     */
    template<
            class T,
            class = std::enable_if_t<std::is_convertible_v<T&&, bool>>>
    constexpr static_bitset(std::initializer_list<T> entries) {
        if (entries.size() > size()) {
            throw_exception(std::invalid_argument("too many elements"));
        }
        size_type i = 0;
        for (auto v : entries) {
            set_unsafe(i, v);
            ++i;
        }
    }

    /**
     * @brief returns the number of available bits.
     * @return the number of available bits
     */
    [[nodiscard]] constexpr size_type size() const noexcept {
        return nbits;
    }

    /**
     * @brief returns the specified bit.
     * @param position the bit position (0-origin)
     * @return true if the target bit is `1`
     * @return false if the target bit is `0`
     * @throws std::out_of_range if the position is out of range
     */
    [[nodiscard]] constexpr bool get(size_type position) const {
        check_range(position);
        return get_unsafe(position);
    }

    /// @copydoc get()
    [[nodiscard]] constexpr bool test(size_type position) const {
        return get(position);
    }

    /**
     * @brief returns the reference of the target bit.
     * @param position the bit position (0-origin)
     * @return the reference of the target bit
     */
    [[nodiscard]] constexpr reference operator[](size_type position) {
        check_range(position);
        return reference { *this, position };
    }

    /// @copydoc get()
    [[nodiscard]] constexpr bool operator[](size_type position) const {
        return get(position);
    }

    /**
     * @brief sets the target bit.
     * @param position the bit position (0-origin)
     * @param value the value to set
     */
    constexpr void set(size_type position, bool value = true) {
        check_range(position);
        set_unsafe(position, value);
    }

    /**
     * @brief sets the target bit to `0`.
     * @param position the bit position (0-origin)
     */
    constexpr void reset(size_type position) {
        set(position, false);
    }

    /**
     * @brief flips the target bit.
     * @param position the bit position (0-origin)
     */
    constexpr void flip(size_type position) {
        check_range(position);
        flip_unsafe(position);
    }

    /**
     * @brief returns the target bit value without bound checking.
     * @param position the bit position (0-origin)
     * @return the target bit value
     * @warning undefined behavior if the target position is out of range
     */
    [[nodiscard]] constexpr bool get_unsafe(size_type position) const noexcept {
        auto&& block = blocks_[block_offset(position)];
        auto mask = bit_mask(position);
        return (block & mask) != 0;
    }

    /**
     * @brief sets a value into the target bit without bound checking.
     * @param position the bit position (0-origin)
     * @param value the value to set
     * @warning undefined behavior if the target position is out of range
     */
    constexpr void set_unsafe(size_type position, bool value = true) noexcept {
        auto&& block = blocks_[block_offset(position)];
        auto mask = bit_mask(position);
        if (value) {
            block |= mask;
        } else {
            block &= ~mask;
        }
    }

    /**
     * @brief flips the target bit without bound checking.
     * @param position the bit position (0-origin)
     */
    constexpr void flip_unsafe(size_type position) noexcept {
        auto&& block = blocks_[block_offset(position)];
        auto mask = bit_mask(position);
        block ^= mask;
    }

    /**
     * @brief returns the number of set bits.
     * @return the number of set bits.
     */
    [[nodiscard]] constexpr size_type count() const noexcept {
        size_type result = 0;
        for (size_type i = 0; i < nbits; ++i) {
            if (get_unsafe(i)) {
                ++result;
            }
        }
        return result;
    }

    /**
     * @brief returns whether or not the all bits are `1`.
     * @return true if the all bits are `1`, or there are no available bits
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool all() const noexcept {
        for (size_type i = 0; i < nbits; ++i) {
            if (!get_unsafe(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief returns whether or not there is `1` in this set.
     * @return true if there is one or more `1` in this
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool any() const noexcept {
        for (size_type i = 0; i < nbits; ++i) {
            if (get_unsafe(i)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @brief returns whether or not the all bits are `0`.
     * @return true if the all bits are `0`, or there are no available bits
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool none() const noexcept {
        return !any();
    }

    /**
     * @brief returns the position of the first occurrence of the given value.
     * @param value the value to find for
     * @return the left-most `1` position (0-origin)
     * @return npos if there are no `1` in this
     */
    [[nodiscard]] constexpr size_type find_first(bool value = true) const noexcept {
        return find(0, value);
    }

    /**
     * @brief returns the position of the next occurrence of the given value.
     * @param position the exclusive starting position (0-origin)
     * @param value the value to find for
     * @return the left-most `1` position from the starting position (0-origin)
     * @return npos if there are no `1` after the starting position
     */
    [[nodiscard]] constexpr size_type find_next(size_type position, bool value = true) const noexcept {
        return find(position + 1, value);
    }

    /**
     * @brief returns the position of the next occurrence of the value.
     * @param start the inclusive starting position (0-origin)
     * @param value the value to find for
     * @return the left-most `1` position from the starting position (0-origin)
     * @return npos if there are no `1` after the starting position
     */
    [[nodiscard]] constexpr size_type find(size_type start = 0, bool value = true) const noexcept {
        for (size_type i = start; i < nbits; ++i) {
            if (get_unsafe(i) == value) {
                return i;
            }
        }
        return npos;
    }

    /**
     * @brief returns whether or not the both are equivalent.
     * @param a the first element
     * @param b the second element
     * @return true if the both are equivalent
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator==(static_bitset const& a, static_bitset const& b) noexcept {
        return a.blocks_ == b.blocks_;
    }

    /**
     * @brief returns whether or not the both are different.
     * @param a the first element
     * @param b the second element
     * @return true if the both are different
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator!=(static_bitset const& a, static_bitset const& b) noexcept {
        return a.blocks_ != b.blocks_;
    }

    /**
     * @brief compares two sets in lexicographic order.
     * @param a the first element
     * @param b the second element
     * @return `< 0` if `a < b`
     * @return `= 0` if `a = b`
     * @return `> 0` if `a > b`
     */
    [[nodiscard]] friend constexpr int compare(static_bitset const& a, static_bitset const& b) noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            auto ae = a.blocks_[i];
            auto be = b.blocks_[i];
            if (ae < be) {
                return -1;
            }
            if (ae > be) {
                return +1;
            }
        }
        return 0;
    }

    /**
     * @brief compares two sets in lexicographic order.
     * @param a the first element
     * @param b the second element
     * @return true if `a < b`
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator<(static_bitset const& a, static_bitset const& b) noexcept {
        return compare(a, b) < 0;
    }

    /**
     * @brief compares two sets in lexicographic order.
     * @param a the first element
     * @param b the second element
     * @return true if `a > b`
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator>(static_bitset const& a, static_bitset const& b) noexcept {
        return compare(a, b) > 0;
    }

    /**
     * @brief compares two sets in lexicographic order.
     * @param a the first element
     * @param b the second element
     * @return true if `a <= b`
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator<=(static_bitset const& a, static_bitset const& b) noexcept {
        return compare(a, b) <= 0;
    }

    /**
     * @brief compares two sets in lexicographic order.
     * @param a the first element
     * @param b the second element
     * @return true if `a >= b`
     * @return false otherwise
     */
    [[nodiscard]] friend constexpr bool operator>=(static_bitset const& a, static_bitset const& b) noexcept {
        return compare(a, b) >= 0;
    }

    /**
     * @brief returns whether or not this is subset of the target one.
     * @param other the target
     * @return true if this is subset of the target one
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool is_subset_of(static_bitset const& other) const noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            auto union_ = blocks_[i] | other.blocks_[i];
            if (union_ != other.blocks_[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief return whether or not this and the given one have any `1` bit in the same position.
     * @param other the target
     * @return true if any bit is `1` both this and target
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool intersects(static_bitset const& other) const noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            if ((blocks_[i] & other.blocks_[i]) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * @brief sets all bits to `1`.
     * @return this
     */
    constexpr static_bitset& set() noexcept {
        reset();
        flip();
        return *this;
    }

    /**
     * @brief sets all bits to `0`.
     * @return this
     */
    constexpr static_bitset& reset() noexcept {
        blocks_ = {};
        return *this;
    }

    /**
     * @brief flips all bits.
     * @return this
     */
    constexpr static_bitset& flip() noexcept {
        for (size_type i = 0; i < nbits; ++i) {
            flip_unsafe(i);
        }
        return *this;
    }

    /**
     * @brief sets bits to `1` which is set to `1` in the given one.
     * @param other the target bit set
     * @return this
     */
    constexpr static_bitset& operator|=(static_bitset const& other) noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            blocks_[i] |= other.blocks_[i];
        }
        return *this;
    }

    /**
     * @brief sets bits to `0` which is set to `0` in the given one.
     * @param other the target bit set
     * @return this
     */
    constexpr static_bitset& operator&=(static_bitset const& other) noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            blocks_[i] &= other.blocks_[i];
        }
        return *this;
    }

    /**
     * @brief sets bits to `0` which is set to `1` in the given one.
     * @param other the target bit set
     * @return this
     */
    constexpr static_bitset& operator-=(static_bitset const& other) noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            blocks_[i] &= ~other.blocks_[i];
        }
        return *this;
    }

    /**
     * @brief flips bits which is set to `1` in the given one.
     * @param other the target bit set
     * @return this
     */
    constexpr static_bitset& operator^=(static_bitset const& other) noexcept {
        for (size_type i = 0; i < nblocks; ++i) {
            blocks_[i] ^= other.blocks_[i];
        }
        return *this;
    }

private:
    std::array<block_type, nblocks> blocks_ {};

    constexpr void check_range(size_type position) const {
        if (position >= size()) {
            throw_exception(std::out_of_range("invalid bit position"));
        }
    }

    [[nodiscard]] constexpr size_type block_offset(size_type position) const {
        return position / nbits_in_block;
    }

    [[nodiscard]] constexpr size_type bit_offset(size_type position) const {
        return position % nbits_in_block;
    }

    [[nodiscard]] constexpr size_type bit_mask(size_type position) const {
        return size_type{1} << bit_offset(position);
    }
};

/**
 * @brief returns the complement of the given bitset.
 * @tparam N the number of available bits
 * @param value the target bitset
 * @return the complement
 */
template<std::size_t N>
constexpr static_bitset<N> operator~(static_bitset<N> const& value) noexcept {
    auto r = value;
    r.flip();
    return r;
}

/**
 * @brief returns the intersection of the bitset pair.
 * @tparam N the number of available bits
 * @param a the first element
 * @param b the second element
 * @return the intersection
 */
template<std::size_t N>
constexpr static_bitset<N> operator&(static_bitset<N> const& a, static_bitset<N> const& b) noexcept {
    auto r = a;
    r &= b;
    return r;
}

/**
 * @brief returns the union of the bitset pair.
 * @tparam N the number of available bits
 * @param a the first element
 * @param b the second element
 * @return the union
 */
template<std::size_t N>
constexpr static_bitset<N> operator|(static_bitset<N> const& a, static_bitset<N> const& b) noexcept {
    auto r = a;
    r |= b;
    return r;
}

/**
 * @brief returns the difference of the bitset pair.
 * @tparam N the number of available bits
 * @param a the first element
 * @param b the second element
 * @return the difference
 */
template<std::size_t N>
constexpr static_bitset<N> operator-(static_bitset<N> const& a, static_bitset<N> const& b) noexcept {
    auto r = a;
    r -= b;
    return r;
}

/**
 * @brief returns the symmetric difference of the bitset pair.
 * @tparam N the number of available bits
 * @param a the first element
 * @param b the second element
 * @return the symmetric difference
 */
template<std::size_t N>
constexpr static_bitset<N> operator^(static_bitset<N> const& a, static_bitset<N> const& b) noexcept {
    auto r = a;
    r ^= b;
    return r;
}

/**
 * @brief appends string representation of the given value.
 * @tparam N the number of available bits
 * @param out the output stream
 * @param value the target value
 * @return the output stream
 */
template<std::size_t N>
inline std::ostream& operator<<(std::ostream& out, static_bitset<N> const& value) {
    out << '{';
    bool cont = false;
    for (std::size_t i = 0; i < N; ++i) {
        if (cont) {
            out << ", ";
        }
        cont = true;
        if (value.get_unsafe(i)) {
            out << '1';
        } else {
            out << '0';
        }
    }
    out << '}';
    return out;
}

} // namespace takatori::util
