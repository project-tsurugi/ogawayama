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

#ifndef WORKER_H_
#define WORKER_H_

#include "umikongo/api.h"
#include "ogawayama/common/channel_stream.h"
#include "ogawayama/common/row_queue.h"

namespace ogawayama::server {

class Worker {
 public:
    Worker(umikongo::Database *, ogawayama::common::SharedMemory *, std::size_t);
    ~Worker() = default;
    void run();
    void execute_query(std::string_view, std::size_t);
 private:
    umikongo::Database *db_;
    ogawayama::common::SharedMemory *shared_memory_ptr_;
    std::size_t id_;

    std::unique_ptr<ogawayama::common::ChannelStream> request_;
    std::unique_ptr<ogawayama::common::ChannelStream> result_;

    std::unique_ptr<umikongo::Transaction> transaction_;
    umikongo::Context* context_;
    std::vector<ogawayama::stub::Metadata> metadatas_;
    std::vector<std::unique_ptr<ogawayama::common::RowQueue>> row_queues_{};
};

}  // ogawayama::server
    
#endif  // WORKER_H_
