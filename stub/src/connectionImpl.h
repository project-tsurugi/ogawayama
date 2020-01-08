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
#pragma once

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/common/parameter_set.h"
#include "transactionImpl.h"
#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Connection::Impl class
 */
class Connection::Impl
{
public:
    Impl(Connection *, std::size_t);
    ~Impl();
    ErrorCode confirm();

    ErrorCode begin(TransactionPtr &transaction);
    ErrorCode prepare(std::string_view sql, PreparedStatementPtr &prepared);

    void get_channel_stream(ogawayama::common::ChannelStream * & channel) const
    {
        channel = channel_.get();
    }

    std::size_t get_id() const { return pgprocno_; }
    
    ogawayama::common::ParameterSet * get_parameters() { return parameters_.get(); }

    auto get_shm4_row_queue() const { return shm4_row_queue_.get(); }
    void get_result_sets(std::vector<std::shared_ptr<ResultSet>> * & result_sets) const
    {
        result_sets = result_sets_.get();
    }

private:
    Connection *envelope_;

    std::size_t pgprocno_;
    std::unique_ptr<ogawayama::common::ChannelStream> channel_;
    std::unique_ptr<ogawayama::common::ParameterSet> parameters_;
    std::size_t sid_{};

    std::unique_ptr<ogawayama::common::SharedMemory> shm4_row_queue_;
    std::unique_ptr<std::vector<std::shared_ptr<ResultSet>>> result_sets_;
};

}  // namespace ogawayama::stub
