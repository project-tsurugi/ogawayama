#pragma once

#include <ostream>
#include <stdexcept>

#include <boost/exception/enable_error_info.hpp>

#include <takatori/util/stacktrace.h>

// FIXME: move to common core module
namespace takatori::util {

/**
 * @brief throws the given exception.
 * @tparam T the exception type
 * @param exception the exception to throw
 */
template<class T>
[[noreturn]] void throw_exception(T const& exception) {
    auto enhanced = ::boost::enable_error_info(exception);
    throw std::move(enhanced); // NOLINT(cert-err60-cpp)
}

} // namespace takatori::util
