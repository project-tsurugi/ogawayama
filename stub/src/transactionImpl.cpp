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

#include "transactionImpl.h"

namespace ogawayama::stub {

/**
 * @brief connect to the DB and get Transaction class
 * @param connection returns a connection class
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_query(std::string query, std::unique_ptr<ResultSet> &result_set) {
    result_set = std::make_unique<ResultSet>();
    return ErrorCode::OK;
}

/**
 * @brief execute a statement.
 * @param statement the SQL statement string
 * @return true in error, otherwise false
 */
ErrorCode Transaction::Impl::execute_statement(std::string statement) {
    return ErrorCode::OK;
}

/**
 * @brief constructor of Transaction class
 */
Transaction::Transaction()
    : transaction_(std::make_unique<Transaction::Impl>()){
}

}  // namespace ogawayama::stub
