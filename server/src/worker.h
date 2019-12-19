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

#include <future>
#include <thread>

#include "umikongo/api.h"
#include "ogawayama/stub/api.h"
#include "ogawayama/common/channel_stream.h"
#include "ogawayama/common/row_queue.h"
#include "ogawayama/common/parameter_set.h"

namespace ogawayama::server {

class Worker {
    class Cursor {
    public:
        void clear() {
            iterator_ = nullptr;
            executable_ = nullptr;
        }
        std::unique_ptr<ogawayama::common::RowQueue> row_queue_{};
        std::unique_ptr<umikongo::Iterator> iterator_{};
        std::unique_ptr<umikongo::ExecutableStatement> executable_{};
    };

 public:
    Worker(umikongo::Database *, ogawayama::common::SharedMemory *, std::size_t);
    ~Worker() {
        clear();
        prepared_statements_.clear();
        if(thread_.joinable()) thread_.join();
    }
    void run();
    void execute_statement(std::string_view);
    bool execute_query(std::string_view, std::size_t);
    void next(std::size_t);

    void prepare(std::string_view, std::size_t);
    void execute_prepared_statement(std::size_t);
    bool execute_prepared_query(std::size_t, std::size_t);

    friend int backend_main(int, char **);

 private:
    umikongo::Database *db_;
    ogawayama::common::SharedMemory *shared_memory_ptr_;
    std::size_t id_;

    std::unique_ptr<ogawayama::common::ChannelStream> channel_;
    std::unique_ptr<ogawayama::common::ParameterSet> parameters_;

    std::unique_ptr<umikongo::Transaction> transaction_;
    umikongo::Context* context_;
    std::vector<Cursor> cursors_;
    std::vector<std::unique_ptr<umikongo::PreparedStatement>> prepared_statements_{};

    std::packaged_task<void()> task_;
    std::future<void> future_;
    std::thread thread_{};

    void send_metadata(std::size_t);
    void set_params(umikongo::PreparedStatement::Parameters *);
    void clear();
};

}  // ogawayama::server
    
#endif  // WORKER_H_
