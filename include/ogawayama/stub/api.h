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
#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <variant>
#include <boost/property_tree/ptree.hpp>

#include <ogawayama/stub/metadata.h>
#include <ogawayama/stub/error_code.h>
#include <ogawayama/stub/transaction_option.h>
#include <ogawayama/stub/Command.h>

#include <manager/message/receiver.h>
#include <manager/message/message.h>
#include <manager/message/status.h>

using MetadataPtr = ogawayama::stub::Metadata const*;
using TYPE = ogawayama::stub::Metadata::ColumnType::Type;
using ERROR_CODE = ogawayama::stub::ErrorCode;

namespace ogawayama::stub {

class ResultSet;
class Transaction;
class PreparedStatement;
class Connection;
class Stub;
 
/**
 * @Brief Result of a query.
 */
class ResultSet{
    class Impl;

public:
    /**
     * @brief Construct a new object.
     */
    ResultSet(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~ResultSet();

    /**
     * @brief get metadata for the result set.
     * @param metadata returns the metadata class
     * @return error code defined in error_code.h
     */
    ErrorCode get_metadata(MetadataPtr&);

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
    ErrorCode next_column(T& value);

private:
    std::unique_ptr<Impl> impl_;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

    friend class Stub;
    friend class Connection;
    friend class Transaction;
};

}  // namespace ogawayama::stub

using ResultSetPtr = std::shared_ptr<ogawayama::stub::ResultSet>;


namespace ogawayama::stub {

using value_type = std::variant<std::monostate, std::int32_t, std::int64_t, float, double, std::string, date_type, time_type, timestamp_type, timetz_type, timestamptz_type>;
using parameters_type = std::vector<std::pair<std::string, value_type>>;

/**
 * @brief Information about a parameter set for a prepared statement.
 */
class PreparedStatement {
    class Impl;

public:
    /**
     * @brief Construct a new object.
     */
    PreparedStatement(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~PreparedStatement();

private:
    std::unique_ptr<Impl> impl_;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

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
    class Impl;

public:
    /**
     * @brief Construct a new object.
     */
    Transaction(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~Transaction();

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(std::string_view);

    /**
     * @brief execute a prepared statement.
     * @param pointer to the prepared statement
     * @param the parameters to be used for execution of the prepared statement
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(PreparedStatementPtr&, parameters_type&);

    /**
     * @brief execute a query.
     * @param query the SQL query string
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(std::string_view, ResultSetPtr&);

    /**
     * @brief execute a prepared query.
     * @param pointer to the prepared query
     * @param the parameters to be used for execution of the prepared statement
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(PreparedStatementPtr&, parameters_type&, ResultSetPtr&);

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
    std::unique_ptr<Impl> impl_;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }
    
    friend class Stub;
    friend class Connection;
    friend class ResultSet;
    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub

using TransactionPtr = std::unique_ptr<ogawayama::stub::Transaction>;


namespace ogawayama::stub {

using placeholders_type = std::vector<std::pair<std::string, Metadata::ColumnType::Type>>;

/**
 * @brief Information about a connection.
 */
class Connection : public manager::message::Receiver {
    class Impl;

public:
    /**
     * @brief Construct a new object.
     */
    Connection(std::unique_ptr<Impl> impl);

    /**
     * @brief destructs this object.
     */
    ~Connection();

    /**
     * @brief begin a transaction and get Transaction class.
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(TransactionPtr&);

    /**
     * @brief begin a transaction and get Transaction class.
     * @param ptree the transaction option
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(const boost::property_tree::ptree&, TransactionPtr&);

    /**
     * @brief prepare SQL statement.
     * @param SQL statement
     * @param prepared statement returns a prepared statement class
     * @return error code defined in error_code.h
     */
    ErrorCode prepare(std::string_view, const placeholders_type&, PreparedStatementPtr&);

    /**
     * @brief implements begin_ddl() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_begin_ddl(const int64_t mode) const;

    /**
     * @brief implements end_ddl() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_end_ddl() const;

    /**
     * @brief implements receive_create_table() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_create_table(const manager::metadata::ObjectIdType object_id) const;

    /**
     * @brief implements drop_table() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_drop_table(const manager::metadata::ObjectIdType object_id) const;

    /**
     * @brief implements receive_create_index() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_create_index(const manager::metadata::ObjectIdType object_id) const;

    /**
     * @brief implements drop_index() procedure
     * @return Status defined in message-broker/include/manager/message/status.h
     */
    manager::message::Status receive_drop_index(const manager::metadata::ObjectIdType object_id) const;

private:
    std::unique_ptr<Impl> impl_;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

    friend class Stub;
    friend class PreparedStatement::Impl;
    friend class Transaction::Impl;
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
     * @brief connect to the DB and get Connection class.
     * @param supposed to be given MyProc->pgprocno for the first param
     * @param connection returns a connection class
     * @return error code defined in error_code.h
     */
    ErrorCode get_connection(ConnectionPtr&, std::size_t);
    ErrorCode get_connection(std::size_t n, ConnectionPtr& connection) { return get_connection(connection, n); }  // only for backwark compatibility

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;

    /**
     * @brief get the impl class
     * @return a pointer to the impl class
     */
    auto get_impl() { return impl_.get(); }

    friend class Connection;
    friend class Connection::Impl;
    friend class Transaction;
    friend class Transaction::Impl;
    friend class ResultSet;
    friend class ResultSet::Impl;
};

}  // namespace ogawayama::stub


namespace ogawayama::common::param {
static const std::string SHARED_MEMORY_NAME = "tsurugi";
}  // namespace ogawayama::common::param

using StubPtr = std::unique_ptr<ogawayama::stub::Stub>;
ERROR_CODE make_stub(StubPtr&, std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME);
inline static StubPtr make_stub(std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME) { return std::make_unique<ogawayama::stub::Stub>(name); }  // only for backwark compatibility
