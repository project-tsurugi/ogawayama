/*
 * Copyright 2019-2023 Project Tsurugi.
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
#ifndef TPCC_SCALE_H_
#define TPCC_SCALE_H_

#include <cstdint>

namespace ogawayama {
namespace tpcc {

struct Scale {
    /** Number of warehouses. Does not grow dynamically */
    std::uint16_t warehouses;

    /** Number of items per warehouse. Does not grow dynamically  */
    std::uint32_t items;

    /** Number of districts per warehouse. Does not grow dynamically  */
    std::uint8_t districts;

    /** Number of customers per district. Does not grow dynamically  */
    std::uint32_t customers;

    /** Number of orders per district. Does grow dynamically. */
    std::uint32_t orders;
};

extern struct Scale *scale, tiny, normal;

}  // namespace tpcc
}  // namespace ogawayama

#endif  // TPCC_SCALE_H_
