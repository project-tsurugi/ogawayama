/*
 * Copyright 2022-2024 Project Tsurugi.
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

#include <map>
#include <utility>
#include <string>

#include <ogawayama/stub/error_code.h>
#include <jogasaki/proto/sql/response.pb.h>
#include <tateyama/proto/diagnostics.pb.h>

namespace ogawayama::transport {

static const std::map<::jogasaki::proto::sql::error::Code, std::pair<std::string, std::int64_t>> error_map = {  // NOLINT

    {::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION, {"SQL_SERVICE_EXCEPTION", 1000}},

    {::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION, {"SQL_SERVICE_EXCEPTION", 1000}},

    {::jogasaki::proto::sql::error::Code::SQL_EXECUTION_EXCEPTION, {"SQL_EXECUTION_EXCEPTION", 2000}},

    {::jogasaki::proto::sql::error::Code::CONSTRAINT_VIOLATION_EXCEPTION, {"CONSTRAINT_VIOLATION_EXCEPTION", 2001}},

    {::jogasaki::proto::sql::error::Code::UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, {"UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION", 2002}},

    {::jogasaki::proto::sql::error::Code::NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION, {"NOT_NULL_CONSTRAINT_VIOLATION_EXCEPTION", 2003}},

    {::jogasaki::proto::sql::error::Code::REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION, {"REFERENTIAL_INTEGRITY_CONSTRAINT_VIOLATION_EXCEPTION", 2004}},

    {::jogasaki::proto::sql::error::Code::CHECK_CONSTRAINT_VIOLATION_EXCEPTION, {"CHECK_CONSTRAINT_VIOLATION_EXCEPTION", 2005}},

    {::jogasaki::proto::sql::error::Code::EVALUATION_EXCEPTION, {"EVALUATION_EXCEPTION", 2010}},

    {::jogasaki::proto::sql::error::Code::VALUE_EVALUATION_EXCEPTION, {"VALUE_EVALUATION_EXCEPTION", 2011}},

    {::jogasaki::proto::sql::error::Code::SCALAR_SUBQUERY_EVALUATION_EXCEPTION, {"SCALAR_SUBQUERY_EVALUATION_EXCEPTION", 2012}},

    {::jogasaki::proto::sql::error::Code::TARGET_NOT_FOUND_EXCEPTION, {"TARGET_NOT_FOUND_EXCEPTION", 2014}},

    {::jogasaki::proto::sql::error::Code::TARGET_ALREADY_EXISTS_EXCEPTION, {"TARGET_ALREADY_EXISTS_EXCEPTION", 2016}},

    {::jogasaki::proto::sql::error::Code::INCONSISTENT_STATEMENT_EXCEPTION, {"INCONSISTENT_STATEMENT_EXCEPTION", 2018}},

    {::jogasaki::proto::sql::error::Code::RESTRICTED_OPERATION_EXCEPTION, {"RESTRICTED_OPERATION_EXCEPTION", 2020}},

    {::jogasaki::proto::sql::error::Code::DEPENDENCIES_VIOLATION_EXCEPTION, {"DEPENDENCIES_VIOLATION_EXCEPTION", 2021}},

    {::jogasaki::proto::sql::error::Code::WRITE_OPERATION_BY_RTX_EXCEPTION, {"WRITE_OPERATION_BY_RTX_EXCEPTION", 2022}},

    {::jogasaki::proto::sql::error::Code::LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION, {"LTX_WRITE_OPERATION_WITHOUT_WRITE_PRESERVE_EXCEPTION", 2023}},

    {::jogasaki::proto::sql::error::Code::READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION, {"READ_OPERATION_ON_RESTRICTED_READ_AREA_EXCEPTION", 2024}},

    {::jogasaki::proto::sql::error::Code::INACTIVE_TRANSACTION_EXCEPTION, {"INACTIVE_TRANSACTION_EXCEPTION", 2025}},

    {::jogasaki::proto::sql::error::Code::PARAMETER_EXCEPTION, {"PARAMETER_EXCEPTION", 2027}},

    {::jogasaki::proto::sql::error::Code::UNRESOLVED_PLACEHOLDER_EXCEPTION, {"UNRESOLVED_PLACEHOLDER_EXCEPTION", 2028}},

    {::jogasaki::proto::sql::error::Code::LOAD_FILE_EXCEPTION, {"LOAD_FILE_EXCEPTION", 2030}},

    {::jogasaki::proto::sql::error::Code::LOAD_FILE_NOT_FOUND_EXCEPTION, {"LOAD_FILE_NOT_FOUND_EXCEPTION", 2031}},

    {::jogasaki::proto::sql::error::Code::LOAD_FILE_FORMAT_EXCEPTION, {"LOAD_FILE_FORMAT_EXCEPTION", 2032}},

    {::jogasaki::proto::sql::error::Code::DUMP_FILE_EXCEPTION, {"DUMP_FILE_EXCEPTION", 2033}},

    {::jogasaki::proto::sql::error::Code::DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION, {"DUMP_DIRECTORY_INACCESSIBLE_EXCEPTION", 2034}},

    {::jogasaki::proto::sql::error::Code::SQL_LIMIT_REACHED_EXCEPTION, {"SQL_LIMIT_REACHED_EXCEPTION", 2036}},

    {::jogasaki::proto::sql::error::Code::TRANSACTION_EXCEEDED_LIMIT_EXCEPTION, {"TRANSACTION_EXCEEDED_LIMIT_EXCEPTION", 2037}},

    {::jogasaki::proto::sql::error::Code::SQL_REQUEST_TIMEOUT_EXCEPTION, {"SQL_REQUEST_TIMEOUT_EXCEPTION", 2039}},

    {::jogasaki::proto::sql::error::Code::DATA_CORRUPTION_EXCEPTION, {"DATA_CORRUPTION_EXCEPTION", 2041}},

    {::jogasaki::proto::sql::error::Code::SECONDARY_INDEX_CORRUPTION_EXCEPTION, {"SECONDARY_INDEX_CORRUPTION_EXCEPTION", 2042}},

    {::jogasaki::proto::sql::error::Code::REQUEST_FAILURE_EXCEPTION, {"REQUEST_FAILURE_EXCEPTION", 2044}},

    {::jogasaki::proto::sql::error::Code::TRANSACTION_NOT_FOUND_EXCEPTION, {"TRANSACTION_NOT_FOUND_EXCEPTION", 2045}},

    {::jogasaki::proto::sql::error::Code::STATEMENT_NOT_FOUND_EXCEPTION, {"STATEMENT_NOT_FOUND_EXCEPTION", 2046}},

    {::jogasaki::proto::sql::error::Code::INTERNAL_EXCEPTION, {"INTERNAL_EXCEPTION", 2048}},

    {::jogasaki::proto::sql::error::Code::UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, {"UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION", 2050}},

    {::jogasaki::proto::sql::error::Code::BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION, {"BLOCKED_BY_HIGH_PRIORITY_TRANSACTION_EXCEPTION", 2052}},

    {::jogasaki::proto::sql::error::Code::INVALID_RUNTIME_VALUE_EXCEPTION, {"INVALID_RUNTIME_VALUE_EXCEPTION", 2054}},

    {::jogasaki::proto::sql::error::Code::VALUE_OUT_OF_RANGE_EXCEPTION, {"VALUE_OUT_OF_RANGE_EXCEPTION", 2056}},

    {::jogasaki::proto::sql::error::Code::VALUE_TOO_LONG_EXCEPTION, {"VALUE_TOO_LONG_EXCEPTION", 2058}},

    {::jogasaki::proto::sql::error::Code::INVALID_DECIMAL_VALUE_EXCEPTION, {"INVALID_DECIMAL_VALUE_EXCEPTION", 2060}},

//    reserved 42 to 100;

    {::jogasaki::proto::sql::error::Code::COMPILE_EXCEPTION, {"COMPILE_EXCEPTION", 3000}},

    {::jogasaki::proto::sql::error::Code::SYNTAX_EXCEPTION, {"SYNTAX_EXCEPTION", 3001}},

    {::jogasaki::proto::sql::error::Code::ANALYZE_EXCEPTION, {"ANALYZE_EXCEPTION", 3002}},

    {::jogasaki::proto::sql::error::Code::TYPE_ANALYZE_EXCEPTION, {"TYPE_ANALYZE_EXCEPTION", 3003}},

    {::jogasaki::proto::sql::error::Code::SYMBOL_ANALYZE_EXCEPTION, {"SYMBOL_ANALYZE_EXCEPTION", 3004}},

    {::jogasaki::proto::sql::error::Code::VALUE_ANALYZE_EXCEPTION, {"VALUE_ANALYZE_EXCEPTION", 3005}},

    {::jogasaki::proto::sql::error::Code::UNSUPPORTED_COMPILER_FEATURE_EXCEPTION, {"UNSUPPORTED_COMPILER_FEATURE_EXCEPTION", 3010}},

//    reserved 108 to 200;

    {::jogasaki::proto::sql::error::Code::CC_EXCEPTION, {"CC_EXCEPTION", 4000}},

    {::jogasaki::proto::sql::error::Code::OCC_EXCEPTION, {"OCC_EXCEPTION", 4001}},

    {::jogasaki::proto::sql::error::Code::OCC_READ_EXCEPTION, {"OCC_READ_EXCEPTION", 4010}},

    {::jogasaki::proto::sql::error::Code::CONFLICT_ON_WRITE_PRESERVE_EXCEPTION, {"CONFLICT_ON_WRITE_PRESERVE_EXCEPTION", 4015}},

    {::jogasaki::proto::sql::error::Code::OCC_WRITE_EXCEPTION, {"OCC_WRITE_EXCEPTION", 4011}},

    {::jogasaki::proto::sql::error::Code::LTX_EXCEPTION, {"LTX_EXCEPTION", 4003}},

    {::jogasaki::proto::sql::error::Code::LTX_READ_EXCEPTION, {"LTX_READ_EXCEPTION", 4013}},

    {::jogasaki::proto::sql::error::Code::LTX_WRITE_EXCEPTION, {"LTX_WRITE_EXCEPTION", 4014}},

    {::jogasaki::proto::sql::error::Code::RTX_EXCEPTION, {"RTX_EXCEPTION", 4005}},

    {::jogasaki::proto::sql::error::Code::BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION, {"BLOCKED_BY_CONCURRENT_OPERATION_EXCEPTION", 4007}}
};

static const std::map<::tateyama::proto::diagnostics::Code, std::pair<std::string, std::int64_t>> framework_error_map = {  // NOLINT

    {::tateyama::proto::diagnostics::Code::UNKNOWN, {"unknown error was occurred in the server.", 0}},

    {::tateyama::proto::diagnostics::Code::SYSTEM_ERROR, {"the server system is something wrong.", 100}},

    {::tateyama::proto::diagnostics::Code::UNSUPPORTED_OPERATION, {"the requested operation is not supported.", 101}},

    {::tateyama::proto::diagnostics::Code::ILLEGAL_STATE, {"I/O error was occurred in the server.", 102}},

    {::tateyama::proto::diagnostics::Code::ILLEGAL_STATE, {"operation was requested in illegal or inappropriate time.", 102}},

    {::tateyama::proto::diagnostics::Code::IO_ERROR, {"I/O error was occurred in the server.", 103}},

    {::tateyama::proto::diagnostics::Code::OUT_OF_MEMORY, {"the server is out of memory.", 104}},

    {::tateyama::proto::diagnostics::Code::RESOURCE_LIMIT_REACHED, {"the server reached resource limit.", 105}},

    {::tateyama::proto::diagnostics::Code::AUTHENTICATION_ERROR, {"authentication was failed.", 201}},

    {::tateyama::proto::diagnostics::Code::PERMISSION_ERROR, {"request is not permitted.", 202}},

    {::tateyama::proto::diagnostics::Code::ACCESS_EXPIRED, {"access right has been expired.", 203}},

    {::tateyama::proto::diagnostics::Code::REFRESH_EXPIRED, {"refresh right has been expired.", 204}},

    {::tateyama::proto::diagnostics::Code::BROKEN_CREDENTIAL, {"credential information is broken.", 205}},

    {::tateyama::proto::diagnostics::Code::SESSION_CLOSED, {"the current session is already closed.", 301}},

    {::tateyama::proto::diagnostics::Code::SESSION_EXPIRED, {"the current session is expired.", 302}},

    {::tateyama::proto::diagnostics::Code::SERVICE_NOT_FOUND, {"the destination service was not found.", 401}},

    {::tateyama::proto::diagnostics::Code::SERVICE_UNAVAILABLE, {"the destination service was not found.", 402}},

    {::tateyama::proto::diagnostics::Code::OPERATION_CANCELED, {"operation was canceled by user or system.", 403}},

    {::tateyama::proto::diagnostics::Code::INVALID_REQUEST, {"the service received a request message with invalid payload.", 501}}
};

} // namespace ogawayama::transport
