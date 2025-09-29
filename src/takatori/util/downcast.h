#pragma once

#include <type_traits>
#include <utility>

#include "infect_qualifier.h"

namespace takatori::util {

/**
 * @brief apply reference narrowing conversion to the given object.
 * @tparam T the destination type
 * @tparam U the source type
 * @param object the target object
 * @return the type casting result
 * @return nullptr if the input is nullptr or narrowing conversion was failed
 */
template<class T, class U>
inline infect_const_t<U, T>* downcast(U* object) {
    return dynamic_cast<infect_const_t<U, T>*>(object); // NOLINT
}

/**
 * @brief apply reference narrowing conversion to the given object.
 * @tparam T the destination type
 * @tparam U the source type
 * @param object the target object
 * @return the type casting result
 * @throws std::bad_cast if narrowing conversion was failed
 */
template<class T, class U>
inline infect_qualifier_t<U, T>&& downcast(U&& object) {
    return dynamic_cast<infect_qualifier_t<U, T>&&>(object); // NOLINT
}

/**
 * @brief apply reference narrowing conversion to the given object, without type checking.
 * @tparam T the destination type
 * @tparam U the source type
 * @param object the target object
 * @return the type casting result
 * @return nullptr if the input is nullptr
 * @warning undefined behavior if narrowing conversion was failed
 */
template<class T, class U>
constexpr inline infect_const_t<U, T>* unsafe_downcast(U* object) {
#if !defined(NDEBUG)
    return downcast<T>(object); // NOLINT
#else
    return static_cast<infect_const_t<U, T>*>(object); // NOLINT
#endif // !defined(NDEBUG)
}

/**
 * @brief apply reference narrowing conversion to the given object, without type checking.
 * @tparam T the destination type
 * @tparam U the source type
 * @param object the target object
 * @return the type casting result
 * @warning undefined behavior if narrowing conversion was failed
 */
template<class T, class U>
constexpr inline infect_qualifier_t<U, T>&& unsafe_downcast(U&& object) {
#if !defined(NDEBUG)
    return downcast<T>(std::forward<U>(object)); // NOLINT
#else
    return static_cast<infect_qualifier_t<U, T>&&>(object);
#endif // !defined(NDEBUG)
}

} // namespace takatori::util
