#include <takatori/util/string_builder.h>

namespace takatori::util {

std::string string_builder::str() const {
    return buffer_.str();
}

std::ostringstream& string_builder::buffer() noexcept {
    return buffer_;
}

std::ostringstream const& string_builder::buffer() const noexcept {
    return buffer_;
}

std::string operator<<(string_builder& builder, string_builder::to_string_t) {
    return builder.str();
}

std::string operator<<(string_builder const& builder, string_builder::to_string_t) {
    return builder.str();
}

std::string operator<<(string_builder&& builder, string_builder::to_string_t) {
    return builder.str();
}

} // namespace takatori::util
