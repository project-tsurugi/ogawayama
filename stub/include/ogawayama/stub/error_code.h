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
     * @brief Encountered server failure.
     */
    SERVER_FAILURE,

};

}  // namespace ogawayama::stub

#endif  // STUB_ERROR_CODE_H_
