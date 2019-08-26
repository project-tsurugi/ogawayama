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
#ifndef SHARED_MEMORY_H_
#define SHARED_MEMORY_H_

namespace ogawayama::common {

struct param {

static constexpr std::size_t SEGMENT_SIZE = 100<<20; // 100 MiB (tantative)

static constexpr std::size_t BUFFER_SIZE = 4096;     // 4K byte (tantative)
static constexpr std::size_t MAX_NAME_LENGTH = 32;   // 64 chars (tantative, but probably enough)

static constexpr std::size_t QUEUE_SIZE = 32;        // 32 rows (tantative)

static constexpr char const * SHARED_MEMORY_NAME = "ogawayama";

static constexpr char const * server = "server";
static constexpr char const * request = "request";
static constexpr char const * result = "result";
static constexpr char const * result_set = "resultset";

};
 
};  // namespace ogawayama::common

#endif //  SHARED_MEMORY_H_
