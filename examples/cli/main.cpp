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
#include <iostream>

#include "gflags/gflags.h"

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/stub/api.h"
#include "stubImpl.h"
#include "transactionImpl.h"

DEFINE_string(dbname, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT
DEFINE_string(statement, "", "SQL statement");  // NOLINT
DEFINE_string(query, "", "SQL query");  // NOLINT
DEFINE_int32(schema, -1, "object id for recieve_message()");  // NOLINT

void prt_err(int line)
{
    std::cerr << "Error at " << line << ", error was falling into default case" << std::endl;
}
void prt_err(int line, ERROR_CODE err)
{
    std::cerr << "Error at " << line << ", error was " << error_name(err) << std::endl;
}

int main(int argc, char **argv) {
    // command arguments
    gflags::SetUsageMessage("a cli tool for ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    StubPtr stub;
    if (FLAGS_statement != "" || FLAGS_query != "" || FLAGS_schema >= 0) {
        ERROR_CODE err = make_stub(stub, (FLAGS_dbname).c_str());
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
    }

    if (FLAGS_schema >= 0) {
        ConnectionPtr connection;
        ERROR_CODE err;

        err = stub->get_connection(connection, 12);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        err = connection->get_impl()->create_table(static_cast<std::size_t>(FLAGS_schema));
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
    }

    if (FLAGS_statement != "") {
        ConnectionPtr connection;
        ERROR_CODE err;

        err = stub->get_connection(connection, 12);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        TransactionPtr transaction;
        err = connection->begin(transaction);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        std::cerr << "execute_statement \"" << FLAGS_statement << "\"" << std::endl;
        err = transaction->execute_statement(FLAGS_statement);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        err = transaction->commit();
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
    }

    if (FLAGS_query != "") {
        ConnectionPtr connection;
        ERROR_CODE err;

        err = stub->get_connection(connection, 12);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        TransactionPtr transaction;
        
        err = connection->begin(transaction);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        ResultSetPtr result_set;
        std::cerr << "execute_query \"" << FLAGS_query << "\"" << std::endl;
        err = transaction->execute_query(FLAGS_query, result_set);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        
        MetadataPtr metadata;
        err = result_set->get_metadata(metadata);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }

        while(true) {
            ERROR_CODE err = result_set->next();
            switch (err) {
            case ERROR_CODE::OK: {
                std::cout << "| ";
                for (auto t: metadata->get_types()) {
                    switch (t.get_type()) {
                    case TYPE::INT16: {
                        std::int16_t v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::INT32: {
                        std::int32_t v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::INT64: {
                        std::int64_t v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::FLOAT32: {
                        float v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::FLOAT64: {
                        double v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::TEXT: {
                        std::string_view v;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    default: {
                        prt_err(__LINE__); return 1;
                    }
                    }
                }
                std::cout << std::endl;
                break;
            }
            case ERROR_CODE::END_OF_ROW: {
                std::cout << "=== end of row ===" << std::endl;
                err = transaction->commit();
                if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
                goto finish;
            }
            default: {
                prt_err(__LINE__); return 1;
            }
            }
        }
    }
  finish:

    return 0;
}
