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
#include <string_view>

#include "ogawayama/stub/metadata.h"
#include "ogawayama/stub/error_code.h"

namespace ogawayama::stub {

class Stub;
class Connection;
class Transaction;
class ResultSet;
 
/**
 * @brief Information about a connection.
 */
class Connection {
public:
    /**
     * @brief Construct a new object.
     */
    Connection(Stub *);

    /**
     * @brief destructs this object.
     */
    ~Connection() noexcept = default;

    /**
     * @brief get the object to which this belongs
     * @return stub objext
     */
    auto get_parent() { return stub_; }

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

    /**
     * @brief begin a transaction and get Transaction class.
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(std::unique_ptr<Transaction> &transaction);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Stub *stub_;
    friend class Stub;
    friend class Transaction;
    friend class ResultSet;
};

/**
 * @brief Information about a transaction.
 */
class Transaction {
public:
    /**
     * @brief Construct a new object.
     */
    Transaction(Connection *);

    /**
     * @brief destructs this object.
     */
    ~Transaction() noexcept = default;

    /**
     * @brief get the object to which this belongs
     * @return connection objext
     */
    auto get_parent() { return connection_; }

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(std::string query, std::shared_ptr<ResultSet> &result_set);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(std::string statement);

    /**
     * @brief commit the current transaction.
     * @return error code defined in error_code.h
     */
    ErrorCode commit();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Connection *connection_;
    friend class Stub;
    friend class Connection;
    friend class ResultSet;
};

/**
 * @brief Result of a query.
 */
class ResultSet{
public:
    /**
     * @brief Construct a new object.
     */
    ResultSet(Transaction *);

    /**
     * @brief destructs this object.
     */
    ~ResultSet() noexcept = default;

    /**
     * @brief get the object to which this belongs
     * @return transaction objext
     */
    auto get_parent() { return transaction_; }

    /**
     * @brief get metadata for the result set.
     * @param metadata returns the metadata class
     * @return error code defined in error_code.h
     */
    ErrorCode get_metadata(std::unique_ptr<Metadata> &metadata);

    /**
     * @brief move current to the next tuple.
     * @param eof returns true when current reaches end of tuple (invalid tuple)
     * @return error code defined in error_code.h
     */
    ErrorCode next();

    /**
     * @brief get value in integer from the current row.
     * @param index culumn number, begins from one
     * @param value returns the value
     * @return error code defined in error_code.h
     */
    template<typename T>
    ErrorCode next_column(T &value);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Transaction *transaction_;
    friend class Stub;
    friend class Connection;
    friend class Transaction;
};

/**
 * @brief environment for server connection.
 */
class Stub {
public:
    /**
     * @brief Construct a new object.
     */
    Stub(std::string_view, bool);

    /**
     * @brief destructs this object.
     */
    ~Stub() noexcept = default;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }
    
    /**
     * @brief connect to the DB and get Connection class.
     * @param connection returns a connection class
     * @return error code defined in error_code.h
     */
    ErrorCode get_connection(std::unique_ptr<Connection> &connection);

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    friend class Connection::Impl;
    friend class Transaction::Impl;
    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub

#endif  // STUB_API_H_
