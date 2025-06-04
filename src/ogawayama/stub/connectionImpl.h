/*
 * Copyright 2019-2025 Project Tsurugi.
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

#include <ogawayama/stub/api.h>
#include "ogawayama/stub/table_metadata_adapter.h"
#include "ogawayama/transport/transport.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Connection::Impl class
 */
class Connection::Impl
{
public:
    Impl(Stub::Impl*, std::string_view, std::size_t, tateyama::authentication::credential_handler&);
    ~Impl();

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    /**
     * @brief begin transaction
     * @param reference to a TransactionPtr
     * @return error code defined in error_code.h
     */
    ErrorCode begin(TransactionPtr&);

    /**
     * @brief begin transaction
     * @param ptree the transaction option
     * @param reference to a TransactionPtr
     * @return error code defined in error_code.h
     */
    ErrorCode begin(const boost::property_tree::ptree&, TransactionPtr&);

    /**
     * @brief prepare statement
     * @param sql statement
     * @param reference to a PreparedSatementPtr
     * @return error code defined in error_code.h
     */
    ErrorCode prepare(std::string_view, const placeholders_type&, PreparedStatementPtr&);

    /**
     * @brief get the error of the last SQL executed
     * @param code returns the error code reported by the tsurugidb
     * @return error code defined in error_code.h
     */
    ErrorCode tsurugi_error(tsurugi_error_code& code);

    /**
     * @brief get table metadata
     * @param table_name the table name
     * @param table_metadata the std::unique_ptr of table_metadata_adapter (output parameter)
     * @return error code defined in error_code.h
     */
    ErrorCode get_table_metadata(const std::string&, TableMetadataPtr& table_metadata);

    /**
     * @brief request list tables and get table_list class.
     * @param table_list returns a table_list_adapter class
     * @return error code defined in error_code.h
     */
    ErrorCode get_list_tables(TableListPtr& table_list);

    /**
     * @brief request get search path and get search_path class.
     * @param sp returns a search_path class
     * @return error code defined in error_code.h
     */
     ErrorCode get_search_path(SearchPathPtr& sp);

private:
    Stub::Impl* manager_;
    std::string session_id_;
    tateyama::common::wire::session_wire_container wire_;
    tateyama::bootstrap::wire::transport transport_;
    std::size_t pgprocno_;

    std::vector<ResultSet::Impl> result_sets_{};

    ErrorCode hello();

    friend class Stub::Impl;
};

}  // namespace ogawayama::stub
