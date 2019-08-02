/*
 * Copyright 2019-2019 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "stub.h"

namespace ogawayama::stub {

/**
 * @brief get value in integer from the column in the current row
 * @param index culumn number, begins from one
 * @param value returns the value
 * @return true in error, otherwise false
 */
bool
ResultSet::get_int64(std::size_t index, std::int64_t &value)
{
    try {
        value = std::get<std::int64_t>(current_->get_value(index));
    }
    catch (std::bad_variant_access& e) {
        return true;
    }
    return false;
}

}  // namespace ogawayama::stub
