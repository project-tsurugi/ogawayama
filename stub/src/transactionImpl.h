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
#ifndef TRANSACTIONIMPL_H_
#define TRANSACTIONIMPL_H_

#include "stubImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Connection::Impl class
 */
class Transaction::Impl
{
public:
    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return true in error, otherwise false
     */
    ErrorCode execute_query(std::string query, std::unique_ptr<ResultSet> &result_set);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return true in error, otherwise false
     */
    ErrorCode execute_statement(std::string statement);
};

}  // namespace ogawayama::stub

#endif  // TRANSACTIONIMPL_H_
