#pragma once

#include <type_traits>

namespace takatori::util {

/**
 * @brief infect reference qualifier from the source type into the destination type.
 * @tparam From the source type
 * @tparam To the destination type
 */
template<class From, class To>
struct infect_reference : std::conditional<
        std::is_lvalue_reference_v<From>,
        std::add_lvalue_reference_t<To>,
        std::conditional_t<
                std::is_rvalue_reference_v<From>,
                std::add_rvalue_reference_t<To>,
                To>> {};

/// @copydoc infect_reference
template<class From, class To>
using infect_reference_t = typename infect_reference<From, To>::type;

/**
 * @brief infect "const" qualifier from the source type into the destination type.
 * @tparam From the source type
 * @tparam To the destination type
 */
template<class From, class To>
struct infect_const : std::conditional<
        std::is_const_v<From>,
        std::add_const_t<To>,
        To> {};

/// @copydoc infect_const
template<class From, class To>
using infect_const_t = typename infect_const<From, To>::type;

/**
 * @brief infect reference qualifier from the source type into the destination type.
 * @details If the destination type is already reference, this has no effect.
 * @tparam From the source type
 * @tparam To the destination type
 */
template<class From, class To>
struct infect_qualifier : infect_reference<
        From,
        infect_const_t<std::remove_reference_t<From>, To>> {};

/// @copydoc infect_qualifier
template<class From, class To>
using infect_qualifier_t = typename infect_qualifier<From, To>::type;

} // namespace takatori::util
