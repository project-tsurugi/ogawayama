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

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "ogawayama/stub/metadata.h"
#include "ogawayama/stub/error_code.h"
#include "ogawayama/stub/Command.h"

#include "manager/message/receiver.h"
#include "manager/message/message.h"
#include "manager/message/status.h"

using MetadataPtr = ogawayama::stub::Metadata const *;
using TYPE = ogawayama::stub::Metadata::ColumnType::Type;
using ERROR_CODE = ogawayama::stub::ErrorCode;

namespace ogawayama::stub {

class ResultSet;
class Transaction;
class PreparedStatement;
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
 * @brief Information about a parameter set for a prepared statement.
 */
class PreparedStatement {
public:
    /**
     * @brief Construct a new object.
     */
    PreparedStatement(Connection*, std::size_t);

    /**
     * @brief destructs this object.
     */
    ~PreparedStatement();

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

    void set_parameter(std::int16_t);
    void set_parameter(std::int32_t);
    void set_parameter(std::int64_t);
    void set_parameter(float);
    void set_parameter(double);
    void set_parameter(std::string_view);
    void set_parameter();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    Connection *manager_;
    friend class Connection;
    friend class Transaction;
};

}  // namespace ogawayama::stub

using PreparedStatementPtr = std::unique_ptr<ogawayama::stub::PreparedStatement>;


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
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(std::string_view);

    /**
     * @brief execute a prepared statement.
     * @param pointer to the prepared statement
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(PreparedStatement *);

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(std::string_view, ResultSetPtr &);

    /**
     * @brief execute a prepared query.
     * @param pointer to the prepared query
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(PreparedStatement *, ResultSetPtr &);

    /**
     * @brief execute a create table statement (with metadata handling).
     * @param statement the SQL statement string
     * @return error code defined in error_code.h
     */
    ErrorCode execute_create_table(std::string_view);

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
class Connection : public manager::message::Receiver {
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

    /**
     * @brief prepare SQL statement.
     * @param SQL statement
     * @param prepared statement returns a prepared statement class
     * @return error code defined in error_code.h
     */
    ErrorCode prepare(std::string_view, PreparedStatementPtr &);

    /**
     * @brief implements begin_ddl() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_begin_ddl(int64_t mode);

    /**
     * @brief implements end_ddl() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_end_ddl();

    /**
     * @brief implements receive_create_table() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_create_table(metadata::ObjectIdType object_id);

    /**
     * @brief implements drop_table() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_drop_table(metadata::ObjectIdType object_id);

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
    ErrorCode get_connection(ConnectionPtr &, std::size_t);
    ErrorCode get_connection(std::size_t n, ConnectionPtr &connection) { return get_connection(connection, n); }  // only for backwark compatibility

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
    friend class Connection::Impl;
    friend class Transaction::Impl;
    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub

namespace ogawayama::common::param {
static const std::string SHARED_MEMORY_NAME = "tateyama";
}  // namespace ogawayama::common::param

using StubPtr = std::unique_ptr<ogawayama::stub::Stub>;
ERROR_CODE make_stub(StubPtr &, std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME);
inline static StubPtr make_stub(std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME) { return std::make_unique<ogawayama::stub::Stub>(name); }  // only for backwark compatibility
