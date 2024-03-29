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
#ifndef STUBIMPL_H_
#define STUBIMPL_H_

#include <memory>

#include "boost/interprocess/managed_shared_memory.hpp"
#include "boost/interprocess/allocators/allocator.hpp"

#include "ogawayama/common/channel_stream.h"

#include "ogawayama/stub/api.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Stub::Impl class
 */
class Stub::Impl
{
public:
    Impl(Stub *, std::string_view);
    ErrorCode get_connection(std::size_t, ConnectionPtr &);
    auto get_managed_shared_memory() { return shared_memory_.get(); }
    auto get_managed_shared_memory_ptr() { return shared_memory_->get_managed_shared_memory_ptr(); }
private:
    Stub *envelope_;

    std::unique_ptr<ogawayama::common::SharedMemory> shared_memory_;
};

}  // namespace ogawayama::stub

#endif  // STUBIMPL_H_
