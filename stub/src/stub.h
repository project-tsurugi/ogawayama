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

#ifndef STUB_H_
#define STUB_H_

#include <variant> 
#include <vector> 

#include "ogawayama/stub/api.h"

namespace ogawayama::stub {

using value_type = std::variant<std::int16_t, std::int32_t, std::int64_t, float, double, std::string>;

/**
 * @brief a holder of a set of row value
 */
class Row {
public:
    /**
     * @brief Construct a new object.
     */
    Row() = default;

    /**
     * @brief destructs this object.
     */
    ~Row() noexcept = default;

    /**
     * @brief get value in variant from the column
     * @param index culumn number, begins from one
     * @return value returns the value
     */
    value_type get_value(std::size_t index) {
        return values_.at(index);
    }

private:
    std::vector<value_type> values_;
};

}  // namespace ogawayama::stub

#endif  // STUB_H_
