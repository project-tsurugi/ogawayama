#pragma once

#include <type_traits>

#include "meta_type.h"

namespace takatori::util {

/// @private
namespace impl {

/**
 * @private
 * @brief detects whether or not Template<Args...> is a valid type.
 * @details If the target type - Template<Args...> is valid, then this struct inherits from std::true,
 * and then provides the target type as "detected" property.
 * Otherwise, this struct inherits from std::false_type, and the "detected" property becomes "Default" type.
 * @tparam Default the "detected" property when the Template<Args...> is INVALID
 * @tparam Void always must be void
 * @tparam Template type template to be validated
 * @tparam Args the template arguments to be validated
 */
template<class Default, class Void, template<class...> class Template, class... Args>
struct detector : std::false_type {
    /// @brief the target type, or the default type if detection was failed.
    using detected = Default;
};

/// @private
template<class Default, template<class...> class Template, class... Args>
struct detector<Default, std::void_t<Template<Args...>>, Template, Args...> : std::true_type {
    using detected = Template<Args...>;
};

} // namespace impl

/**
 * @brief represents template type application was failed.
 */
struct detect_failure {};

/**
 * @brief provides whether or not Template<Args...> is a valid type.
 * @tparam Template type template to be validated
 * @tparam Args the template arguments to be validated
 */
template<template<class...> class Template, class... Args>
using is_detected = impl::detector<detect_failure, void, Template, Args...>;

/// @copydoc is_detected
template<template<class...> class Template, class... Args>
constexpr inline bool is_detected_v = is_detected<Template, Args...>::value;

/**
 * @brief provides Template<Args...> only if it is a valid type.
 * @details If it is not a valid type, this becomes meta-type of Default.
 * @tparam Default the result type when the validation was failed
 * @tparam Template type template to be validated
 * @tparam Args the template arguments to be validated
 */
template<class Default, template<class...> class Template, class... Args>
using detect_or = util::meta_type<typename impl::detector<Default, void, Template, Args...>::detected>;

/// @copydoc detect_or
template<class Default, template<class...> class Template, class... Args>
using detect_or_t = typename detect_or<Default, Template, Args...>::type;

/**
 * @brief provides Template<Args...> only if it is a valid type
 * @details If it is not a valid type, this becomes meta-type of not_detected.
 * @tparam Template type template to be validated
 * @tparam Args the template arguments to be validated
 */
template<template<class...> class Template, class... Args>
using detect = detect_or<detect_failure, Template, Args...>;

/// @copydoc detect
template<template<class...> class Template, class... Args>
using detect_t = typename detect<Template, Args...>::type;

} // namespace takatori::util
