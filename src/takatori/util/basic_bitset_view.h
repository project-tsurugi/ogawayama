#pragma once

#include <stdexcept>
#include <type_traits>

#include <cstddef>

#include "details/basic_bit_pointer.h"
#include "details/basic_bit_reference.h"

namespace takatori::util {

/**
 * @brief an unmodifiable bit-set view.
 * @details Individual bit values are packed every 8 units.
 *      Individual bits are arranged from LSB to MSB, from the first octet.
 * @details This does not holds ownership of the memory area.
 * @tparam BlockType the block type
 */
template<class BlockType>
class basic_bitset_view {
public:
    static_assert(std::is_integral_v<BlockType>);

    /// @brief the value type.
    using value_type = bool;

    /// @brief the block type.
    using block_type = BlockType;

    /// @brief the pointer to individual bits.
    using pointer = details::basic_bit_pointer<block_type>;

    /// @brief the reference to individual bits.
    using reference = details::basic_bit_reference<block_type>;

    /// @brief the iterator type.
    using iterator = pointer;

    /// @brief the const iterator type.
    using const_iterator = details::basic_bit_pointer<std::add_const_t<block_type>>;

    /// @brief the block pointer type.
    using block_pointer = std::add_pointer_t<block_type>;

    /// @brief the size and position type.
    using size_type = std::size_t;

    /// @brief the number of bits in block
    static constexpr size_type block_units = sizeof(block_type) * 8;

    /**
     * @brief creates a new empty instance.
     */
    constexpr basic_bitset_view() noexcept = default;

    /**
     * @brief creates a new instance.
     * @param block_data the head octet of this view
     * @param bit_count the available number of bits
     */
    constexpr basic_bitset_view(block_pointer block_data, size_type bit_count) noexcept :
            block_data_ { block_data },
            bit_size_ { bit_count }
    {}

    /**
     * @brief converts block type.
     * @tparam T the source block type
     * @param other the copy source
     */
    template<
            class T,
            class = std::enable_if_t<std::is_constructible_v<
                    typename basic_bitset_view<BlockType>::block_pointer,
                    typename basic_bitset_view<T>::block_pointer>>>
    constexpr basic_bitset_view(basic_bitset_view<T> other) noexcept : // NOLINT
            basic_bitset_view {
                other.block_data(),
                other.size(),
            }
    {}

    /**
     * @brief returns the available number of bits.
     * @return the available number of bits
     */
    [[nodiscard]] constexpr size_type size() const noexcept {
        return bit_size_;
    }

    /**
     * @brief returns whether or not this bitset capacity is empty.
     * @return true if `size() == 0`
     * @return false if `size() > 0`
     */
    [[nodiscard]] constexpr bool empty() const noexcept {
        return bit_size_ == 0;
    }

    /**
     * @brief returns content at the position on this buffer.
     * @param position the content position
     * @return the buffer content
     * @pre `position < size()`
     * @attention undefined behavior if the `position >= size()`
     */
    [[nodiscard]] constexpr reference operator[](size_type position) const noexcept {
        return reference {
            block_data_,
            position,
        };
    }

    /**
     * @brief returns content at the position on this buffer.
     * @param position the content position
     * @return the buffer content
     * @pre `position < size()`
     * @throw std::out_of_range if `position >= size()`
     */
    [[nodiscard]] constexpr reference at(size_type position) const {
        if (position >= bit_size_) {
            throw std::out_of_range("buffer position is out of range");
        }
        return operator[](position);
    }

    /**
     * @brief returns a value iterator that points to the beginning of this buffer.
     * @return an iterator of beginning
     */
    [[nodiscard]] constexpr iterator begin() const noexcept {
        return iterator {
                block_data_,
                0,
        };
    }

    /// @copydoc begin()
    [[nodiscard]] constexpr const_iterator cbegin() const noexcept {
        return const_iterator { begin() };
    }

    /**
     * @brief returns a value iterator that points to the ending of this buffer.
     * @return an iterator of ending
     */
    [[nodiscard]] constexpr iterator end() const noexcept {
        return iterator {
                block_data_,
                size(),
        };
    }

    /// @copydoc end()
    [[nodiscard]] constexpr const_iterator cend() const noexcept {
        return const_iterator { end() };
    }

    /**
     * @brief returns the head of underlying buffer.
     * @return the head of underlying buffer
     */
    [[nodiscard]] constexpr block_pointer block_data() const noexcept {
        return block_data_;
    }

    /**
     * @brief returns the number of bytes in this view.
     * @return the number of bytes in this view
     */
    [[nodiscard]] constexpr size_type block_size() const noexcept {
        return to_block_size(bit_size_);
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, basic_bitset_view value) {
        out << "basic_bitset_view(";
        bool first = true;
        for (bool&& bit : value) {
            if (!first) {
                out << ", ";
            }
            first = false;
            out << (bit? '1' : '0');
        }
        out << ")";
        return out;
    }

private:
    block_pointer block_data_ {};
    size_type bit_size_ {};

    constexpr static size_type to_block_size(size_type bit_size) noexcept {
        return (bit_size + block_units - 1) / block_units;
    }

    constexpr static size_type to_block_offset(size_type bit_position) noexcept {
        return bit_position / block_units;
    }

    constexpr static size_type to_bit_offset(size_type bit_position) noexcept {
        return bit_position % block_units;
    }
};

/**
 * @brief returns whether or not two buffers have equivalent contents.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first buffer
 * @param right the second buffer
 * @return `true` if both have equivalent contents
 * @return `false` otherwise
 */
template<class U, class V>
[[nodiscard]] bool operator==(basic_bitset_view<U> left, basic_bitset_view<V> right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t i = 0, n = left.size(); i < n; ++i) {
        if (left[i] != right[i]) {
            return false;
        }
    }
    return true;
}

/**
 * @brief returns whether or not two buffers have different contents.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first buffer
 * @param right the second buffer
 * @return `true` if both have different contents
 * @return `false` otherwise
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator!=(basic_bitset_view<U> left, basic_bitset_view<V> right) noexcept {
    return !(left == right);
}

} // namespace takatori::util
