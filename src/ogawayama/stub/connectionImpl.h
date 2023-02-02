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
#include "transport.h"

#include "transactionImpl.h"
#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Connection::Impl class
 */
class Connection::Impl
{
public:
    Impl(Stub::Impl*, std::string_view, std::size_t);
    ~Impl();

    ErrorCode confirm();

    /**
     * @brief begin transaction
     * @param reference to a TransactionPtr
     * @return error code defined in error_code.h
     */
    ErrorCode begin(TransactionPtr&);

    /**
     * @brief prepare statement
     * @param sql statement
     * @param reference to a PreparedSatementPtr
     * @return error code defined in error_code.h
     */
    ErrorCode prepare(std::string_view, PreparedStatementPtr&);

    /**
     * @brief relay a create tabe message from the frontend to the server
     * @param table id given by the frontend
     * @return error code defined in error_code.h
     */
    ErrorCode create_table(std::size_t);

    /**
     * @brief relay a drop tabe message from the frontend to the server
     * @param table id given by the frontend
     * @return error code defined in error_code.h
     */
    ErrorCode drop_table(std::size_t);

    /**
     * @brief relay a begin ddl message from the frontend to the server
     * @return error code defined in error_code.h
     */
    ErrorCode begin_ddl();

    /**
     * @brief relay a end ddl from the frontend to the server
     * @return error code defined in error_code.h
     */
    ErrorCode end_ddl();

private:
    Stub::Impl* manager_;
    std::string session_id_;
    tateyama::common::wire::session_wire_container wire_;
    tateyama::bootstrap::wire::transport transport_;
    std::size_t pgprocno_;

    std::vector<ResultSet::Impl> result_sets_{};

    // for old-fashioned link
    std::unique_ptr<ogawayama::common::SharedMemory> shm4_connection_;
    std::unique_ptr<ogawayama::common::ChannelStream> channel_;
};

}  // namespace ogawayama::stub