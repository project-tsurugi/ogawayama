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

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/stub/api.h"

static StubPtr stub;
static ConnectionPtr connection;
static TransactionPtr transaction;
static MetadataPtr metadata;
static ResultSetPtr result_set;

void err_exit(int line)
{
    std::cerr << "Error at " << line << std::endl;
    exit(1);
}

int main() {
    stub = make_stub();
    
    if (stub->get_connection(12, connection) != ERROR_CODE::OK) { err_exit(__LINE__); }

    if (connection->begin(transaction) != ERROR_CODE::OK) { err_exit(__LINE__); }

    if (transaction->execute_query("SELECT * FROM T2", result_set) != ERROR_CODE::OK) { err_exit(__LINE__); }

    if (result_set->get_metadata(metadata) != ERROR_CODE::OK) { err_exit(__LINE__); }

    while(true) {
        switch (result_set->next()) {
        case ERROR_CODE::OK: {
            std::cout << "| ";
            for (auto& t: metadata->get_types()) {
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
            goto finish;
        }
        default: {
            err_exit(__LINE__);
        }
        }
    }

 finish:
    transaction->commit();
    return 0;
}
