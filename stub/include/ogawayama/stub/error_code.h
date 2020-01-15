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
#ifndef STUB_ERROR_CODE_H_
#define STUB_ERROR_CODE_H_

namespace ogawayama::stub {

/**
 * @brief The types of errors that can occur in the data handling with ogawayama stub.
 */
enum class ErrorCode {

    /**
     * @brief Success
     */
    OK = 0,

    /**
     * @brief NULL has been observed as the column value (a result of successful prodessing).
     */
    COLUMN_WAS_NULL,

    /**
     * @brief Current in the ResultSet stepped over the last row (a result of successful prodessing).
     */
    END_OF_ROW,

    /**
     * @brief Current Column in the Row stepped over the last column (a result of successful prodessing).
     */
    END_OF_COLUMN,

    /**
     * @brief the column value has been requested for a different type than the actual type.
     */
    COLUMN_TYPE_MISMATCH,

    /**
     * @brief function not supported.
     */
    UNSUPPORTED,

    /**
     * @brief transaction not started.
     */
    NO_TRANSACTION,

    /**
     * @brief parameter value is invalid.
     */
    INVALID_PARAMETER,

    /**
     * @brief unknown error.
     */
    UNKNOWN,

    /**
     * @brief Encountered server failure.
     */
    SERVER_FAILURE,

    /**
     * @brief for internal use.
     */
    TIMEOUT,

};

constexpr std::string_view error_name(ErrorCode code) {
    switch (code) {
    case ErrorCode::OK: return "OK";
    case ErrorCode::COLUMN_WAS_NULL: return "COLUMN_WAS_NULL";
    case ErrorCode::END_OF_ROW: return "END_OF_ROW";
    case ErrorCode::END_OF_COLUMN: return "END_OF_COLUMN";
    case ErrorCode::COLUMN_TYPE_MISMATCH: return "COLUMN_TYPE_MISMATCH";
    case ErrorCode::UNSUPPORTED: return "UNSUPPORTED";
    case ErrorCode::NO_TRANSACTION: return "NO_TRANSACTION";
    case ErrorCode::INVALID_PARAMETER: return "INVALID_PARAMETER";
    case ErrorCode::UNKNOWN: return "UNKNOWN";
    case ErrorCode::SERVER_FAILURE: return "SERVER_FAILURE";
    case ErrorCode::TIMEOUT: return "TIMEOUT";
    default: return "This ERROR_CODE is illegal";
    }
}

}  // namespace ogawayama::stub

#endif  // STUB_ERROR_CODE_H_
