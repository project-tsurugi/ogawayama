#pragma once

#include <ostream>
#include <stdexcept>
#include <string_view>
#include <type_traits>

#include <cstddef>
#include <cstring>

namespace takatori::util {

/**
 * @brief a buffer view.
 * @details This does not holds ownership of the buffer area.
 * @tparam BlockType the block type
 */
template<class BlockType>
class basic_buffer_view {
public:
    static_assert(std::is_integral_v<BlockType>);

    /// @brief the buffer block type.
    using block_type = BlockType;

    /// @brief the pointer to buffer block.
    using block_pointer = std::add_pointer_t<block_type>;

    /// @brief the byte type.
    using value_type = std::remove_cv_t<block_type>;

    /// @brief the pointer type.
    using pointer = block_pointer;

    /// @brief the constant pointer type.
    using const_pointer = std::add_pointer_t<std::add_const_t<block_type>>;

    /// @brief the reference type.
    using reference = std::add_lvalue_reference_t<block_type>;

    /// @brief the size and position type.
    using size_type = std::size_t;

    /// @brief the iterator type.
    using iterator = pointer;

    /// @brief the constant iterator type.
    using const_iterator = const_pointer;

    /// @brief the difference type.
    using difference_type = std::ptrdiff_t;

    /**
     * @brief creates a new empty instance.
     */
    constexpr basic_buffer_view() noexcept = default;

    /**
     * @brief creates a new instance.
     * @param data the head of this buffer
     * @param size the number of units in the buffer
     */
    constexpr basic_buffer_view(pointer data, size_type size) noexcept :
        data_ { data },
        size_ { size }
    {}

    /**
     * @brief converts block type.
     * @tparam T the source block type
     * @param other the copy source
     */
    template<
            class T,
            class = std::enable_if_t<std::is_constructible_v<
                    typename basic_buffer_view<BlockType>::block_pointer,
                    typename basic_buffer_view<T>::block_pointer>>>
    constexpr basic_buffer_view(basic_buffer_view<T> other) noexcept : // NOLINT
            basic_buffer_view {
                other.block_data(),
                other.size(),
            }
    {}

    /**
     * @brief returns the head of this buffer.
     * @return the head of this buffer.
     */
    [[nodiscard]] constexpr pointer data() const noexcept {
        return data_;
    }

    /**
     * @brief the buffer capacity.
     * @return the number of units available in this buffer
     */
    [[nodiscard]] constexpr size_type size() const noexcept {
        return size_;
    }

    /**
     * @brief returns whether or not this buffer is empty.
     * @return true if this buffer is empty
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool empty() const noexcept {
        return size_ == 0;
    }

    /**
     * @brief returns whether or not this buffer has contents.
     * @return true if this buffer is not empty
     * @return false if this buffer is empty
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return !empty();
    }

    /**
     * @brief returns content at the position on this buffer.
     * @param position the content position
     * @return the buffer content
     * @pre `position < size()`
     * @attention undefined behavior if the `position >= size()`
     */
    [[nodiscard]] constexpr reference operator[](size_type position) const noexcept {
        return *(data_ + position); // NOLINT
    }

    /**
     * @brief returns content at the position on this buffer.
     * @param position the content position
     * @return the buffer content
     * @pre `position < size()`
     * @throw std::out_of_range if `position >= size()`
     */
    [[nodiscard]] constexpr reference at(size_type position) const {
        if (position >= size_) {
            throw std::out_of_range("buffer position is out of range");
        }
        return operator[](position);
    }

    /**
     * @brief returns a slice of this buffer.
     * @param from the beginning position in this buffer
     * @param to the beginning position in this buffer
     * @return a new basic_buffer_view with range of `[from. to)`
     * @throws std::out_of_range if `from > to` or `to >= size()`
     */
    [[nodiscard]] constexpr basic_buffer_view slice(size_type from, size_type to) const {
        if (from > to || to >= size_) {
            throw std::out_of_range("buffer position is out of range");
        }
        return {
            data_ + from, // NOLINT
            to - from,
        };
    }

    /**
     * @brief returns a value iterator that points to the beginning of this buffer.
     * @return an iterator of beginning
     */
    [[nodiscard]] constexpr iterator begin() const noexcept {
        return data_;
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
        return data_ + size_; // NOLINT
    }

    /// @copydoc end()
    [[nodiscard]] constexpr const_iterator cend() const noexcept {
        return const_iterator { end() };
    }

private:
    block_pointer data_ { nullptr };
    size_type size_ { 0 };
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
[[nodiscard]] constexpr bool operator==(basic_buffer_view<U> left, basic_buffer_view<V> right) noexcept {
    constexpr auto left_unit_size = sizeof(typename basic_buffer_view<U>::block_type);
    constexpr auto right_unit_size = sizeof(typename basic_buffer_view<V>::block_type);
    auto left_byte_size = left.size() * left_unit_size;
    auto right_byte_size = right.size() * right_unit_size;
    if (left_byte_size != right_byte_size) {
        return false;
    }
    auto const* left_data = static_cast<void const*>(left.data());
    auto const* right_data = static_cast<void const*>(right.data());
    if (left_data == right_data) {
        return true;
    }
    return std::memcmp(left_data, right_data, left_byte_size) == 0;
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
[[nodiscard]] constexpr bool operator!=(basic_buffer_view<U> left, basic_buffer_view<V> right) noexcept {
    return !(left == right);
}

/**
 * @brief appends string representation of the given value.
 * @tparam U the block type
 * @param out the target output
 * @param value the target value
 * @return the output
 */
template<class U>
std::ostream& operator<<(std::ostream& out, basic_buffer_view<U> value) {
    return out << static_cast<void const*>(value.data()) << "+" << value.size();
}

} // namespace takatori::util
