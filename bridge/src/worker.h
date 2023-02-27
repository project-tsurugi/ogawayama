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
#include <manager/metadata/tables.h>
#include <manager/metadata/metadata.h>
#include <manager/metadata/error_code.h>
#include <manager/metadata/datatypes.h>

namespace ogawayama::bridge {

class listener;

class Worker {
    class Cursor {
    public:
        void clear() {
            result_set_ = nullptr;
            result_set_iterator_ = nullptr;
        }
    private:
        std::unique_ptr<jogasaki::api::result_set> result_set_{};
        std::unique_ptr<jogasaki::api::result_set_iterator> result_set_iterator_{};
        jogasaki::api::statement_handle prepared_{};

        friend class Worker;
    };

 public:
    Worker(jogasaki::api::database&, std::string&, std::string_view, std::size_t);
    ~Worker() {
        clear_all();
        if(thread_.joinable()) thread_.join();
    }

    Worker(Worker const& other) = delete;
    Worker& operator=(Worker const& other) = delete;
    Worker(Worker&& other) noexcept = delete;
    Worker& operator=(Worker&& other) noexcept = delete;

    void run();
    friend class listener;

 private:
    void execute_statement(std::string_view);
    bool execute_query(std::string_view, std::size_t);
    void next(std::size_t);
    void prepare(std::string_view, std::size_t);
    void execute_prepared_statement(std::size_t);
    bool execute_prepared_query(std::size_t, std::size_t);
    void deploy_metadata(std::size_t);
    void withdraw_metadata(std::size_t);
    void begin_ddl();
    void end_ddl();

    jogasaki::api::database& db_;
    std::size_t id_;
    std::string& dbname_;

    std::unique_ptr<ogawayama::common::SharedMemory> shm4_connection_;
    std::unique_ptr<ogawayama::common::ChannelStream> channel_;
//    std::unique_ptr<ogawayama::common::ParameterSet> parameters_;

    jogasaki::api::transaction_handle transaction_handle_{};
    std::vector<Cursor> cursors_;
    std::vector<jogasaki::api::statement_handle> prepared_statements_{};

    std::packaged_task<void()> task_;
    std::future<void> future_;
    std::thread thread_{};

//    std::unique_ptr<ogawayama::common::SharedMemory> shm4_row_queue_;

    void send_metadata(std::size_t);
    void set_params(std::unique_ptr<jogasaki::api::parameter_set>&);
    void clear_transaction() {
        cursors_.clear();
        if (transaction_handle_) {
            if (auto rc = db_.destroy_transaction(transaction_handle_); rc != jogasaki::status::ok) {
                LOG(ERROR) << "fail to db_.destroy_transaction(transaction_handle_)";
            }
        }
        transaction_handle_ = jogasaki::api::transaction_handle();
    }
    void clear_all() {
        clear_transaction();
//        shm4_row_queue_ = nullptr;
        prepared_statements_.clear();
    }
};

}  // ogawayama::bridge
