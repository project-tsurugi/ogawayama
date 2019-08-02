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
#ifndef STUB_API_H_
#define STUB_API_H_

#include <iostream>
#include <memory>
#include <string>


#include "ogawayama/stub/metadata.h"

namespace ogawayama::stub {

class Connection;
class Transaction;
class ResultSet;
class Row;
    
/**
 * @brief environment for server connection.
 */
class Stub {
public:
    /**
     * @brief Construct a new object.
     */
    Stub() = default;

    /**
     * @brief destructs this object.
     */
    ~Stub() noexcept = default;

    /**
     * @brief connect to the DB and get Connection class
     * @param connection returns a connection class
     * @return true in error, otherwise false
     */
    bool get_connection(std::unique_ptr<Connection> &connection);
};

/**
 * @brief Information about a connection.
 */
class Connection {
    /**
     * @brief Construct a new object.
     */
    Connection() = default;

    /**
     * @brief destructs this object.
     */
    ~Connection() noexcept = default;

    /**
     * @brief begin a transaction and get Transaction class
     * @param transaction returns a transaction class
     * @return true in error, otherwise false
     */
    bool begin(std::unique_ptr<Transaction> &transaction);
};

/**
 * @brief Information about a transaction.
 */
class Transaction {
    /**
     * @brief Construct a new object.
     */
    Transaction() = default;

    /**
     * @brief destructs this object.
     */
    ~Transaction() noexcept = default;

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return true in error, otherwise false
     */
    bool execute_query(std::string query, std::unique_ptr<ResultSet> &result_set);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return true in error, otherwise false
     */
    bool execute_statement(std::string statement);

    /**
     * @brief commit the current transaction.
     * @return true in error, otherwise false
     */
    bool commit();
};

/**
 * @brief Result of a query.
 */
class ResultSet{
    
    /**
     * @brief Construct a new object.
     */
    ResultSet() = default;

    /**
     * @brief destructs this object.
     */
    ~ResultSet() noexcept = default;

    /**
     * @brief move current to the next tuple.
     * @param eof returns true when current reaches end of tuple (invalid tuple)
     * @return true in error, otherwise false
     */
    bool next(bool &eot);

    /**
     * @brief get value in integer from the current row
     * @param index culumn number, begins from one
     * @param value returns the value
     * @return true in error, otherwise false
     */
    bool get_int64(std::size_t index, std::int64_t &value);

    /**
     * @brief get value in float from the current row
     * @param index culumn number, begins from one
     * @param value returns the value
     * @return true in error, otherwise false
     */
    bool get_float64(std::size_t index, double &value);

    /**
     * @brief get value in string from the current row
     * @param index culumn number, begins from one
     * @param value returns the value
     * @return true in error, otherwise false
     */
    bool get_text(std::size_t index, std::string &value);

private:
    std::unique_ptr<Row> current_; 

};

}  // namespace ogawayama::stub

#endif  // STUB_API_H_
