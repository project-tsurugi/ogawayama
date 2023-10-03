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
#ifndef TRANSACTIONIMPL_H_
#define TRANSACTIONIMPL_H_

#include "memory"
#include "vector"

#include "connectionImpl.h"
#include "result_setImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of Transaction::Impl class
 */
class Transaction::Impl
{
public:
    Impl(Transaction *);

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return true in error, otherwise false
     */
    ErrorCode execute_query(std::string_view query, std::shared_ptr<ResultSet> &result_set);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return true in error, otherwise false
     */
    ErrorCode execute_statement(std::string_view statement);

    /**
     * @brief commit the current transaction.
     * @return error code defined in error_code.h
     */
    ErrorCode commit();
    
    /**
     * @brief abort the current transaction.
     * @return error code defined in error_code.h
     */
    ErrorCode rollback();

private:
    Transaction *envelope_;

    std::unique_ptr<std::vector<std::shared_ptr<ResultSet>>> result_sets_;
};

}  // namespace ogawayama::stub

#endif  // TRANSACTIONIMPL_H_
