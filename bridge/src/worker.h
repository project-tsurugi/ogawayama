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
#pragma once

#include <future>
#include <thread>

#include <jogasaki/api.h>
#include <ogawayama/stub/api.h>
#include <ogawayama/common/channel_stream.h>
#include <ogawayama/common/row_queue.h>
#include <ogawayama/common/parameter_set.h>
#include <manager/metadata/tables.h>
#include <manager/metadata/metadata.h>
#include <manager/metadata/error_code.h>
#include <manager/metadata/datatypes.h>

namespace ogawayama::bridge {

class fe_provider;

class Worker {
    class Cursor {
    public:
        void clear() {
            result_set_ = nullptr;
//            prepared_ = nullptr;
        }
        std::unique_ptr<ogawayama::common::RowQueue> row_queue_{};
        std::unique_ptr<jogasaki::api::result_set> result_set_{};
        jogasaki::api::statement_handle prepared_{};
    };

 public:
    Worker(jogasaki::api::database&, std::size_t);
    ~Worker() {
        clear_all();
        if(thread_.joinable()) thread_.join();
    }
    void run();
    friend class fe_provider;

 private:
    void execute_statement(std::string_view);
    bool execute_query(std::string_view, std::size_t);
    void next(std::size_t);
    void prepare(std::string_view, std::size_t);
    void execute_prepared_statement(std::size_t);
    bool execute_prepared_query(std::size_t, std::size_t);
    void deploy_metadata(std::size_t);

    jogasaki::api::database& db_;
    std::size_t id_;

    std::unique_ptr<ogawayama::common::SharedMemory> shm4_connection_;
    std::unique_ptr<ogawayama::common::ChannelStream> channel_;
    std::unique_ptr<ogawayama::common::ParameterSet> parameters_;

    jogasaki::api::transaction_handle transaction_handle_{};
    std::vector<Cursor> cursors_;
    std::vector<jogasaki::api::statement_handle> prepared_statements_{};

    std::packaged_task<void()> task_;
    std::future<void> future_;
    std::thread thread_{};

    std::unique_ptr<ogawayama::common::SharedMemory> shm4_row_queue_;

    void send_metadata(std::size_t);
    void set_params(std::unique_ptr<jogasaki::api::parameter_set>&);
    void clear_transaction() {
        cursors_.clear();
//        transaction_ = nullptr;
    }
    void clear_all() {
        clear_transaction();
        shm4_row_queue_ = nullptr;
        prepared_statements_.clear();
    }
};

}  // ogawayama::bridge
