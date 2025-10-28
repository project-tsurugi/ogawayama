#pragma once

#include <type_traits>

#include <cstdlib>

#include "data.h"
#include "type_kind.h"

#include "primitive.h"
#include "decimal.h"
#include "character.h"
#include "octet.h"
#include "bit.h"
#include "date.h"
#include "time_of_day.h"
#include "time_point.h"
#include "datetime_interval.h"
#include "lob.h"
// #include "array.h"
// #include "record.h"
// #include "row_reference.h"
#include "row_id.h"
#include "declared.h"
#include "extension.h"

#include <takatori/util/callback.h>

namespace takatori::type {

/**
 * @brief invoke callback function for individual subclasses of type::data.
 * @details If the actual input object has type of T, this may invoke Callback::operator()(T const&, Args...).
 * @attention You must declare all callback functions for individual subclasses,
 * or declare Callback::operator()(type::data const&, Args...) as "default" callback function.
 * Each return type of callback function must be identical.
 * @tparam Callback the callback object type
 * @tparam Args the callback argument types
 * @param callback the callback object
 * @param object the target object
 * @param args the callback arguments
 * @return the callback result
 */
template<class Callback, class... Args>
inline auto dispatch(Callback&& callback, data const& object, Args&&... args) {
    switch (object.kind()) {
        case boolean::tag: return util::polymorphic_callback<boolean>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case int1::tag: return util::polymorphic_callback<int1>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case int2::tag: return util::polymorphic_callback<int2>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case int4::tag: return util::polymorphic_callback<int4>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case int8::tag: return util::polymorphic_callback<int8>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case float4::tag: return util::polymorphic_callback<float4>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case float8::tag: return util::polymorphic_callback<float8>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case decimal::tag: return util::polymorphic_callback<decimal>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case character::tag: return util::polymorphic_callback<character>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case octet::tag: return util::polymorphic_callback<octet>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case bit::tag: return util::polymorphic_callback<bit>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case date::tag: return util::polymorphic_callback<date>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case time_of_day::tag: return util::polymorphic_callback<time_of_day>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case time_point::tag: return util::polymorphic_callback<time_point>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case datetime_interval::tag: return util::polymorphic_callback<datetime_interval>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case blob::tag: return util::polymorphic_callback<blob>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case clob::tag: return util::polymorphic_callback<clob>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
//        case array::tag: return util::polymorphic_callback<array>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
//        case record::tag: return util::polymorphic_callback<record>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case unknown::tag: return util::polymorphic_callback<unknown>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
//        case row_reference::tag: return util::polymorphic_callback<row_reference>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case row_id::tag: return util::polymorphic_callback<row_id>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case declared::tag: return util::polymorphic_callback<declared>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);
        case extension::tag: return util::polymorphic_callback<extension>(std::forward<Callback>(callback), object, std::forward<Args>(args)...);

        // FIXME: other cases
        default: break;
    }
    std::abort();
}

} // namespace takatori::type
