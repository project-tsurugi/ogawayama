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
#include "tpcc_scale.h"

namespace ogawayama {
namespace tpcc {

struct Scale tiny = {
    .warehouses = 1U,
    .items = 50U,
    .districts = 2U,
    .customers = 30U,
    .orders = 30U,
};

struct Scale normal = {
    .warehouses = 1U,
    .items = 100000U,
    .districts = 10U,
    .customers = 3000U,
    .orders = 3000U,
};
    
struct Scale *scale = &tiny;

}  // namespace tpcc
}  // namespace ogawayama
