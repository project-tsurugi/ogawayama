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
#include <ogawayama/stub/table_metadata.h>
#include <ogawayama/stub/table_list.h>

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
    explicit ResultSet(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~ResultSet();

    ResultSet(const ResultSet&) = delete;
    ResultSet& operator=(const ResultSet&) = delete;
    ResultSet(ResultSet&&) = delete;
    ResultSet& operator=(ResultSet&&) = delete;

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

using value_type = std::variant<std::monostate, std::int32_t, std::int64_t, float, double, std::string, date_type, time_type, timestamp_type, timetz_type, timestamptz_type, decimal_type>;
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
    explicit PreparedStatement(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~PreparedStatement();

    PreparedStatement(const PreparedStatement&) = delete;
    PreparedStatement& operator=(const PreparedStatement&) = delete;
    PreparedStatement(PreparedStatement&&) = delete;
    PreparedStatement& operator=(PreparedStatement&&) = delete;

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
    explicit Transaction(std::unique_ptr<Impl>);

    /**
     * @brief destructs this object.
     */
    ~Transaction();

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string to be executed
     * @param num_rows a reference to a variable to which the number of processes
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(std::string_view statement, std::size_t& num_rows);

    /**
     * @brief execute a prepared statement.
     * @param prepared_statement the prepared statement to be executed
     * @param parameters the parameters to be used for execution of the prepared statement
     * @param num_rows a reference to a variable to which the number of processes
     * @return error code defined in error_code.h
     */
    ErrorCode execute_statement(PreparedStatementPtr& prepared_statement, parameters_type& parameters, std::size_t& num_rows);

    /**
     * @brief execute a statement.
     * @param statement the SQL statement string to be executed
     * @return error code defined in error_code.h
     */
    __attribute__((__deprecated__))
    ErrorCode execute_statement(std::string_view statement);

    /**
     * @brief execute a prepared statement.
     * @param prepared_statement the prepared statement to be executed
     * @param parameters the parameters to be used for execution of the prepared statement
     * @return error code defined in error_code.h
     */
    __attribute__((__deprecated__))
    ErrorCode execute_statement(PreparedStatementPtr& prepared_statement, parameters_type& parameters);

    /**
     * @brief execute a query.
     * @param query the SQL query string to be executed
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(std::string_view query, ResultSetPtr& result_set);

    /**
     * @brief execute a prepared query.
     * @param prepared_query the prepared query to be executed
     * @param parameters the parameters to be used for execution of the prepared statement
     * @param result_set returns a result set of the query
     * @return error code defined in error_code.h
     */
    ErrorCode execute_query(PreparedStatementPtr& prepared_query, parameters_type& parameters, ResultSetPtr& result_set);

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
using TableMetadataPtr = std::unique_ptr<ogawayama::stub::table_metadata>;
using TableListPtr = std::unique_ptr<ogawayama::stub::table_list>;
using SearchPathPtr = std::unique_ptr<ogawayama::stub::search_path>;

namespace ogawayama::stub {

using placeholders_type = std::vector<std::pair<std::string, Metadata::ColumnType::Type>>;

/**
 * @brief Information about a connection.
 */
class Connection {
    class Impl;

public:
    /**
     * @brief Construct a new object.
     */
    explicit Connection(std::unique_ptr<Impl> impl);

    /**
     * @brief destructs this object.
     */
    virtual ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&&) = delete;
    Connection& operator=(Connection&&) = delete;

    /**
     * @brief request begin and get Transaction class.
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(TransactionPtr&);

    /**
     * @brief request begin with transaction option and get Transaction class.
     * @param ptree the transaction option
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(const boost::property_tree::ptree&, TransactionPtr&);

    /**
     * @brief request prepare and get PreparedStatement class.
     * @param SQL statement
     * @param prepared statement returns a prepared statement class
     * @return error code defined in error_code.h
     */
    ErrorCode prepare(std::string_view, const placeholders_type&, PreparedStatementPtr&);

    /**
     * @brief request table metadata and get TableMetadata class.
     * @param table_name the table name
     * @param table_metadata returns a table_metadata class
     * @return error code defined in error_code.h
     */
    ErrorCode get_table_metadata(const std::string& table_name, TableMetadataPtr& table_metadata);

    /**
     * @brief request list tables and get table_list class.
     * @param table_list returns a table_list class
     * @return error code defined in error_code.h
     */
    ErrorCode get_list_tables(TableListPtr& table_list);

    /**
     * @brief request get search path and get search_path class.
     * @param sp returns a search_path class
     * @return error code defined in error_code.h
     */
     ErrorCode get_search_path(SearchPathPtr& sp);

    /**
     * @brief get the error of the last SQL executed
     * @param code returns the error code reported by the tsurugidb
     * @return error code defined in error_code.h
     */
    ErrorCode tsurugi_error(tsurugi_error_code& code);

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
    explicit Stub(std::string_view);

    /**
     * @brief destructs this object.
     */
    ~Stub();

    Stub(Stub const& other) = delete;
    Stub& operator=(Stub const& other) = delete;
    Stub(Stub&& other) noexcept = delete;
    Stub& operator=(Stub&& other) noexcept = delete;

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
using namespace std::string_view_literals;

static const std::string_view SHARED_MEMORY_NAME = "tsurugi"sv;
}  // namespace ogawayama::common::param

using StubPtr = std::unique_ptr<ogawayama::stub::Stub>;
ERROR_CODE make_stub(StubPtr&, std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME);
inline static StubPtr make_stub(std::string_view name = ogawayama::common::param::SHARED_MEMORY_NAME) { return std::make_unique<ogawayama::stub::Stub>(name); }  // only for backwark compatibility
