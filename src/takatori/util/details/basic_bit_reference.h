#pragma once

#include <type_traits>

namespace takatori::util::details {

/**
 * @brief represents a reference of bit in packed bit sequence.
 * @tparam BlockType the block type
 */
template<class BlockType>
class basic_bit_reference {
public:
    static_assert(std::is_integral_v<BlockType>);

    /// @brief the value type of bit.
    using value_type = bool;

    /// @brief the bare block type.
    using block_type = BlockType;

    /// @brief the block value type.
    using block_value_type = std::remove_cv_t<block_type>;

    /// @brief the unsigned type of block value.
    using unsigned_block_value_type = std::make_unsigned_t<block_value_type>;

    /// @brief the block pointer type.
    using block_pointer = std::add_pointer_t<block_type>;

    /// @brief the size and position type.
    using size_type = std::size_t;

    /// @brief the number of bits in block
    static constexpr size_type block_units = sizeof(block_type) * 8;

    /**
     * @brief creates a new object.
     * @param block_data the block data
     * @param bit_position the bit offset on `block_data`
     */
    explicit constexpr basic_bit_reference(
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
                    typename basic_bit_reference<BlockType>::block_pointer,
                    typename basic_bit_reference<T>::block_pointer>>>
    constexpr basic_bit_reference(basic_bit_reference<T> other) noexcept : // NOLINT
            basic_bit_reference {
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
     * @brief returns a bit value.
     * @return `true` if the bit is set
     * @return `false` if the bit is clear
     */
    [[nodiscard]] constexpr operator value_type() const noexcept { // NOLINT
        auto&& block = *block_data_;
        auto bits = static_cast<unsigned_block_value_type>(block);
        return (bits & to_bit_mask(bit_position_)) != 0;
    }

    /**
     * @brief sets a bit value.
     * @param value the bit value: `true` to set bit, or `false` to clear bit
     * @return this
     */
    basic_bit_reference& operator=(value_type value) noexcept {
        static_assert(!std::is_const_v<BlockType>, "block must not be const");
        auto&& block = *block_data_;
        auto bits = static_cast<unsigned_block_value_type>(block);
        if (value) {
            bits |= to_bit_mask(bit_position_);
        } else {
            bits &= ~to_bit_mask(bit_position_);
        }
        block = static_cast<block_value_type>(bits);
        return *this;
    }

    /**
     * @brief flips the bit.
     * @return this
     */
    basic_bit_reference& flip() noexcept {
        static_assert(!std::is_const_v<BlockType>, "block must not be const");
        auto&& block = *block_data_;
        auto bits = static_cast<unsigned_block_value_type>(block);
        bits ^= to_bit_mask(bit_position_);
        block = static_cast<block_value_type>(bits);
        return *this;
    }

private:
    block_pointer block_data_ {};
    size_type bit_position_ {};

    [[nodiscard]] static constexpr size_type to_bit_mask(size_type bit_position) noexcept {
        return 1U << bit_position;
    }
};

} // namespace takatori::util::details
