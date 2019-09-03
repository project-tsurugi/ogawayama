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

using MetadataPtr = ogawayama::stub::Metadata *;
using TYPE = ogawayama::stub::Metadata::ColumnType::Type;
using ERROR_CODE = ogawayama::stub::ErrorCode;

namespace ogawayama::stub {

class ResultSet;
class Transaction;
class Connection;
class Stub;
 
/**
 * @brief Result of a query.
 */
class ResultSet{
public:
    /**
     * @brief Construct a new object.
     */
    ResultSet(Transaction *, std::size_t);

    /**
     * @brief destructs this object.
     */
    ~ResultSet();

    /**
     * @brief get the object to which this belongs
     * @return transaction objext
     */
    auto get_manager() { return manager_; }

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

    /**
     * @brief get metadata for the result set.
     * @param metadata returns the metadata class
     * @return error code defined in error_code.h
     */
    ErrorCode get_metadata(MetadataPtr &);

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
    Transaction *manager_;
    friend class Stub;
    friend class Connection;
    friend class Transaction;
};

}  // namespace ogawayama::stub

using ResultSetPtr = std::shared_ptr<ogawayama::stub::ResultSet>;


namespace ogawayama::stub {

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
    ~Transaction();

    /**
     * @brief get the object to which this belongs
     * @return connection objext
     */
    auto get_manager() { return manager_; }

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }
    
    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(std::string_view, ResultSetPtr &);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(std::string_view);

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
    class Impl;
    std::unique_ptr<Impl> impl_;
    Connection *manager_;
    friend class Stub;
    friend class Connection;
    friend class ResultSet;
};

}  // namespace ogawayama::stub

using TransactionPtr = std::unique_ptr<ogawayama::stub::Transaction>;


namespace ogawayama::stub {

/**
 * @brief Information about a connection.
 */
class Connection {
public:
    /**
     * @brief Construct a new object.
     */
    Connection(Stub *, std::size_t);

    /**
     * @brief destructs this object.
     */
    ~Connection();

    /**
     * @brief get the object to which this belongs
     * @return stub objext
     */
    auto get_manager() { return manager_; }

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
    ErrorCode begin(TransactionPtr &);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Stub *manager_;
    friend class Stub;
    friend class Transaction;
    friend class ResultSet;
};

}  // namespace ogawayama::stub

using ConnectionPtr = std::unique_ptr<ogawayama::stub::Connection>;


namespace ogawayama::stub {

/**
 * @brief environment for server connection.
 */
class Stub {
public:
    /**
     * @brief Construct a new object.
     */
    Stub(std::string_view);

    /**
     * @brief destructs this object.
     */
    ~Stub();

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }
    
    /**
     * @brief connect to the DB and get Connection class.
     * @param supposed to be given MyProc->pgprocno for the first param
     * @param connection returns a connection class
     * @return error code defined in error_code.h
     */
    ErrorCode get_connection(std::size_t, ConnectionPtr &);

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    friend class Connection::Impl;
    friend class Transaction::Impl;
    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub

namespace ogawayama::common::param {
    static constexpr char const * SHARED_MEMORY_NAME = "ogawayama";
}  // namespace ogawayama::common::param

using StubPtr = std::unique_ptr<ogawayama::stub::Stub>;
inline static StubPtr make_stub(std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME) { return std::make_unique<ogawayama::stub::Stub>(name); }

#endif  // STUB_API_H_
