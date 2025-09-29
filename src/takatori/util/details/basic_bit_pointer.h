#pragma once

#include <ostream>
#include <type_traits>

#include "basic_bit_reference.h"

namespace takatori::util::details {

/**
 * @brief represents a pointer to bit in packed bit sequence.
 * @tparam BlockType the block type
 */
template<class BlockType>
class basic_bit_pointer {
public:
    static_assert(std::is_integral_v<BlockType>);

    /// @brief the bit value type.
    using value_type = bool;

    /// @brief the difference type.
    using difference_type = std::ptrdiff_t;

    /// @brief the block type.
    using block_type = BlockType;

    /// @brief the de-referenced type.
    using reference = basic_bit_reference<block_type>;

    /// @brief the block pointer type.
    using block_pointer = std::add_pointer_t<block_type>;

    /// @brief the size and position type.
    using size_type = std::size_t;

    /// @brief the number of bits in block
    static constexpr size_type block_units = sizeof(block_type) * 8;

    // for iterator_traits

    /// @brief just me.
    using pointer = basic_bit_pointer;

    /// @brief iterator category.
    using iterator_category = std::random_access_iterator_tag;

    /**
     * @brief creates a new object.
     * @param block_data the block data
     * @param bit_position the bit offset on `block_data`
     */
    explicit constexpr basic_bit_pointer(
            block_pointer block_data,
            size_type bit_position = {}) noexcept :
        block_data_ { block_data + (bit_position / block_units) },
        bit_position_ { bit_position % block_units }
    {}

    /**
     * @brief converts block type.
     * @tparam T the source block type
     * @param other the copy source
     */
    template<
            class T,
            class = std::enable_if_t<std::is_constructible_v<
                    typename basic_bit_pointer<BlockType>::block_pointer,
                    typename basic_bit_pointer<T>::block_pointer>>>
    constexpr basic_bit_pointer(basic_bit_pointer<T> other) noexcept : // NOLINT
            basic_bit_pointer {
                other.block_data(),
                other.bit_position(),
            }
    {}

    /**
     * @brief returns the block data where this points to.
     * @return the block data position
     */
    [[nodiscard]] constexpr block_pointer block_data() const noexcept {
        return block_data_;
    }

    /**
     * @brief returns the bit offset in the block_data() where this points to.
     * @return the bit offset, from LSB to MSB in the block
     */
    [[nodiscard]] constexpr size_type bit_position() const noexcept {
        return bit_position_;
    }

    /**
     * @brief returns a reference to bit where this points to.
     * @return the reference
     */
    [[nodiscard]] constexpr reference operator*() const noexcept {
        return reference { block_data_, bit_position_ };
    }

    /// @copydoc operator*()
    [[nodiscard]] constexpr reference operator->() const noexcept {
        return reference { block_data_, bit_position_ };
    }

    /**
     * @brief returns whether or not the given pointer points to as same as this points.
     * @param other the target pointer
     * @return `true` if both point the same bit
     * @return `false` otherwise
     */
    [[nodiscard]] constexpr bool operator==(basic_bit_pointer other) {
        return block_data_ == other.block_data_ && bit_position_ == other.bit_position_;
    }

    /**
     * @brief returns whether or not the given pointer points to other from this points.
     * @param other the target pointer
     * @return `true` if both point other bits
     * @return `false` otherwise
     */
    [[nodiscard]] constexpr bool operator!=(basic_bit_pointer other) {
        return !operator==(other);
    }

    /**
     * @brief adds difference of bit position to this.
     * @param difference difference of bit position
     * @return this
     * @attention undefined behavior if the bit position goes out of range from the original bit sequence
     */
    basic_bit_pointer& operator+=(difference_type difference) noexcept {
        auto diff_with_bitpos = difference + static_cast<difference_type>(bit_position_);

        difference_type diff_block {};
        if (diff_with_bitpos >= 0) {
            //  division of positive is equivalent to always floor()
            diff_block = diff_with_bitpos / static_cast<difference_type>(block_units);
        } else {
            // compute floor() for division of negative number
            diff_block = (diff_with_bitpos - static_cast<difference_type>(block_units) + 1)
                    / static_cast<difference_type>(block_units);
        }
        block_data_ += diff_block;

        // overwrite bit_position because reverse_difference always including bit_position
        bit_position_ = static_cast<size_type>(diff_with_bitpos - (diff_block * block_units));
        return *this;
    }

    /**
     * @brief subtracts difference of bit position from this.
     * @param difference difference of bit position
     * @return this
     * @attention undefined behavior if the bit position goes out of range from the original bit sequence
     */
    basic_bit_pointer& operator-=(difference_type difference) noexcept {
        return operator+=(-difference);
    }

    /**
     * @brief adds `1` to bit position.
     * @return this
     */
    basic_bit_pointer& operator++() noexcept {
        return operator+=(1);
    }

    /**
     * @brief adds `1` to bit position.
     * @return a copy of this before this operation was performed
     */
    basic_bit_pointer const operator++(int) noexcept { // NOLINT(readability-const-return-type)
        auto copy = *this;
        this->operator++();
        return copy;
    }

    /**
     * @brief subtracts `1` from bit position.
     * @return this
     */
    basic_bit_pointer& operator--() noexcept {
        return operator-=(1);
    }

    /**
     * @brief subtracts `1` from bit position.
     * @return a copy of this before this operation was performed
     */
    basic_bit_pointer const operator--(int) noexcept { // NOLINT(readability-const-return-type)
        auto copy = *this;
        this->operator--();
        return copy;
    }

    /**
     * @brief adds difference of bit position to the pointer.
     * @param pointer the target pointer
     * @param difference difference of bit position
     * @return the resulting pointer
     * @attention undefined behavior if the bit position goes out of range from the original bit sequence
     */
    friend constexpr basic_bit_pointer operator+(basic_bit_pointer pointer, difference_type difference) noexcept {
        auto copy = pointer;
        return copy += difference;
    }

    /**
     * @brief adds difference of bits position to the pointer.
     * @param pointer the target pointer
     * @param difference difference of bits position
     * @return the resulting pointer
     * @attention undefined behavior if the bit position goes out of range from the original bit sequence
     */
    friend constexpr basic_bit_pointer operator+(difference_type difference, basic_bit_pointer pointer) noexcept {
        return pointer + difference;
    }

    /**
     * @brief subtracts difference of bits position from the pointer.
     * @param pointer the target pointer
     * @param difference difference of bits position
     * @return the resulting pointer
     * @attention undefined behavior if the bit position goes out of range from the original bit sequence
     */
    friend constexpr basic_bit_pointer operator-(basic_bit_pointer pointer, difference_type difference) noexcept {
        return pointer + -difference;
    }

    /**
     * @brief returns the difference of two pointers.
     * @param left the first operand
     * @param right the second operand
     * @return the difference
     * @attention undefined behavior if two pointers does not have the common block data
     */
    friend constexpr difference_type operator-(basic_bit_pointer left, basic_bit_pointer right) noexcept {
        auto block_difference = left.block_data_ - right.block_data_;
        auto bit_difference = static_cast<difference_type>(left.bit_position_)
                - static_cast<difference_type>(right.bit_position_);
        return block_difference * static_cast<difference_type>(block_units) + bit_difference;
    }

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output
     */
    friend std::ostream& operator<<(std::ostream& out, basic_bit_pointer value) {
        return out << static_cast<void const*>(value.block_data_) << "[" << value.bit_position_ << "]";
    }

private:
    block_pointer block_data_ {};
    size_type bit_position_ {};
};

/**
 * @brief returns whether or not two pointers point to the same bit.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if both point to the same bit
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator==(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    return left.block_data() == right.block_data() && left.bit_position() == right.bit_position();
}

/**
 * @brief returns whether or not two pointers point to the same bit.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if each points to the different bit
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator!=(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    return !(left == right);
}

/**
 * @brief returns whether or not the first pointer is less then the second one.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if the first pointer is less then the second one
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator<(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    if (left.block_data() != right.block_data()) {
        return left.block_data() < right.block_data();
    }
    return left.bit_position() < right.bit_position();
}

/**
 * @brief returns whether or not the first pointer is greater then the second one.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if the first pointer is less then the second one
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator>(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    return right < left;
}

/**
 * @brief returns whether or not the first pointer is less then or equal to the second one.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if the first pointer is less then the second one
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator<=(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    return !(right < left);
}

/**
 * @brief returns whether or not the first pointer is greater then or equal to the second one.
 * @tparam U the first block type
 * @tparam V the second block type
 * @param left the first pointer
 * @param right the second pointer
 * @return `true` if the first pointer is less then the second one
 * @return `false` otherwise
 * @attention undefined behavior if two block type is not compatible
 */
template<class U, class V>
[[nodiscard]] constexpr bool operator>=(basic_bit_pointer<U> left, basic_bit_pointer<V> right) noexcept {
    return !(left < right);
}

} // namespace takatori::util::details
