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
#include <glog/logging.h>

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
    google::InstallFailureSignalHandler();

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
        manager::message::Status manager_err(manager::message::ErrorCode::SUCCESS, 0);

        err = stub->get_connection(connection, 12);
        if (err != ERROR_CODE::OK) { prt_err(__LINE__, err); return 1; }
        manager_err = connection->receive_begin_ddl(0);  // mode 0 is dummy
        if (manager_err.get_error_code() != manager::message::ErrorCode::SUCCESS) { prt_err(__LINE__, static_cast<ERROR_CODE>(manager_err.get_sub_error_code())); return 1; }
        manager_err = connection->receive_create_table(static_cast<manager::metadata::ObjectIdType>(FLAGS_schema));
        if (manager_err.get_error_code() != manager::message::ErrorCode::SUCCESS) { prt_err(__LINE__, static_cast<ERROR_CODE>(manager_err.get_sub_error_code())); return 1; }
        manager_err = connection->receive_end_ddl();
        if (manager_err.get_error_code() != manager::message::ErrorCode::SUCCESS) { prt_err(__LINE__, static_cast<ERROR_CODE>(manager_err.get_sub_error_code())); return 1; }
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
                    case TYPE::DATE: {
                        ogawayama::stub::date_type v;  // takatori::datetime::date
                        err = result_set->next_column(v);
                        auto days_since_epoch = v.days_since_epoch();  // using difference_type = std::int64_t;
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << days_since_epoch << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::TIME: {
                        ogawayama::stub::time_type v;  // takatori::datetime::time_of_day
                        err = result_set->next_column(v);
                        auto time_since_epoch = v.time_since_epoch();  // using time_unit = std::chrono::duration<std::uint64_t, std::nano>;
                        std::chrono::seconds sec = std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << sec.count() << "," << std::chrono::duration_cast<std::chrono::microseconds>(v.subsecond()).count() << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::TIMESTAMP: {
                        ogawayama::stub::timestamp_type v;  // using timestamp_type = takatori::datetime::time_point;
                        err = result_set->next_column(v);
                        auto seconds_since_epoch = v.seconds_since_epoch();  // using offset_type = std::chrono::duration<std::int64_t>;
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << seconds_since_epoch.count() << "," << std::chrono::duration_cast<std::chrono::microseconds>(v.subsecond()).count() << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::TIMETZ: {
                        ogawayama::stub::timetz_type v;  // using timetz_type = std::pair<takatori::datetime::time_of_day, std::int32_t>;
                        err = result_set->next_column(v);
                        auto time = v.first;
                        auto offset = v.second;
                        auto time_since_epoch = time.time_since_epoch();  // using time_unit = std::chrono::duration<std::uint64_t, std::nano>;
                        std::chrono::seconds sec = std::chrono::duration_cast<std::chrono::seconds>(time_since_epoch);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << sec.count() << "," << std::chrono::duration_cast<std::chrono::microseconds>(time.subsecond()).count()  << ", " << offset << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::TIMESTAMPTZ: {
                        ogawayama::stub::timestamptz_type v;  // using timestamptz_type = std::pair<takatori::datetime::time_point, std::int32_t>;
                        err = result_set->next_column(v);
                        auto timestamp = v.first;
                        auto offset = v.second;
                        auto seconds_since_epoch = timestamp.seconds_since_epoch();  // using offset_type = std::chrono::duration<std::int64_t>;
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << seconds_since_epoch.count() << "," << std::chrono::duration_cast<std::chrono::microseconds>(timestamp.subsecond()).count() << ", " << offset << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    case TYPE::DECIMAL: {
                        ogawayama::stub::decimal_type v;  // using timestamptz_type = std::pair<takatori::datetime::time_point, std::int32_t>;
                        err = result_set->next_column(v);
                        switch (err) {
                        case ERROR_CODE::OK: std::cout << v.sign() << ":" << v.coefficient_high() << ":" << v.coefficient_low() << ":" << v.exponent() << " | "; break;
                        case ERROR_CODE::COLUMN_WAS_NULL: std::cout << "(null) | "; break;
                        default: prt_err(__LINE__, err); return 1;
                        }
                        break;
                    }
                    default:
                        prt_err(__LINE__); return 1;
                    }
                }
                std::cout << std::endl;
                break;
            }
            case ERROR_CODE::END_OF_ROW: {
                std::cout << "=== end of row ===" << std::endl;
                err = transaction->commit();
                if (err != ERROR_CODE::OK) {
                    prt_err(__LINE__, err); return 1;
                }
                return 0;
            }
            default:
                prt_err(__LINE__); return 1;
            }
        }
    }
    return 0;
}
