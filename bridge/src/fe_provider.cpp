/*
 * Copyright 2019-2021 tsurugi project.
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
#include "fe_provider.h"

namespace ogawayama::bridge::api {

DEFINE_bool(remove_shm, false, "remove the shared memory prior to the execution");  // NOLINT

static bool bridge_ogawayama_entry;

/**
 * @brief add frontend bridge to the component registry
 */
void prepare() {
    bridge_ogawayama_entry = ::tateyama::api::registry<ogawayama::bridge::api::provider>::add("ogawayama", ogawayama::bridge::fe_provider::create);  // NOLINT
}

}  // ogawayama::bridge::api
