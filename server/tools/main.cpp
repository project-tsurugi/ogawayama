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

DEFINE_bool(terminate, false, "terminate commnand");  // NOLINT
DEFINE_string(statement, "", "SQL statement");  // NOLINT
DEFINE_string(query, "", "SQL query");  // NOLINT

void err_exit(int line)
{
    std::cerr << "Error at " << line << std::endl;
    exit(1);
}

int main(int argc, char **argv) {
    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    StubPtr stub = make_stub(ogawayama::common::param::SHARED_MEMORY_NAME);
    if (FLAGS_terminate) {
        stub->get_impl()->get_channel()->get_binary_oarchive() <<
            ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::TERMINATE);
    }

    if (FLAGS_statement != "") {
        ConnectionPtr connection;
        if (stub->get_connection(12, connection) != ERROR_CODE::OK) { err_exit(__LINE__); }
        TransactionPtr transaction;
        if (connection->begin(transaction) != ERROR_CODE::OK) { err_exit(__LINE__); }
        std::cerr << "execute_statement \"" << FLAGS_statement << "\"" << std::endl;
        if (transaction->execute_statement(FLAGS_statement) != ERROR_CODE::OK) { err_exit(__LINE__); }
        transaction->commit();
    }

    if (FLAGS_query != "") {
        ConnectionPtr connection;
        if (stub->get_connection(12, connection) != ERROR_CODE::OK) { err_exit(__LINE__); }
        TransactionPtr transaction;
        if (connection->begin(transaction) != ERROR_CODE::OK) { err_exit(__LINE__); }
        ResultSetPtr result_set;
        std::cerr << "execute_query \"" << FLAGS_query << "\"" << std::endl;
        if (transaction->execute_query(FLAGS_query, result_set) != ERROR_CODE::OK) { err_exit(__LINE__); }
        MetadataPtr metadata;
        if (result_set->get_metadata(metadata) != ERROR_CODE::OK) { err_exit(__LINE__); }

        while(true) {
            switch (result_set->next()) {
            case ERROR_CODE::OK: {
                std::cout << "| ";
                for (auto t: metadata->get_types()) {
                    switch (t.get_type()) {
                    case TYPE::INT16: {
                        std::int16_t v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    case TYPE::INT32: {
                        std::int32_t v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    case TYPE::INT64: {
                        std::int64_t v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    case TYPE::FLOAT32: {
                        float v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    case TYPE::FLOAT64: {
                        double v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    case TYPE::TEXT: {
                        std::string_view v;
                        switch (result_set->next_column(v)) {
                        case ERROR_CODE::OK: std::cout << v << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: err_exit(__LINE__);
                        }
                        break;
                    }
                    default: {
                        err_exit(__LINE__);
                    }
                    }
                }
                std::cout << std::endl;
                break;
            }
            case ERROR_CODE::END_OF_ROW: {
                std::cout << "=== end of row ===" << std::endl;
                transaction->commit();
                goto finish;
            }
            default: {
                err_exit(__LINE__);
            }
            }
        }
    }

 finish:
    return 0;
}
