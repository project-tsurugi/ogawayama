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

#include <cstddef>
#include <vector>

#include <takatori/value/date.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>
#include <takatori/value/decimal.h>

#include "ogawayama/stub/error_code.h"

namespace ogawayama::stub {

/**
 * @brief Provides semantic information of a ResultSet.
 */
class Metadata {
public:
    /**
     * @brief Provides semantic information of a Column.
     */
    class ColumnType {
    public:
        /**
         * @brief represents a type.
         */
        enum class Type {
            /**
             * @brief Pseudotype representing that the column is NULL.
             */
            NULL_VALUE = 0,

            /**
             * @brief 16bit integral number type.
             */
            INT16 = 1,

            /**
             * @brief 32bit integral number type.
             */
            INT32 = 2,

            /**
             * @brief 64bit integral number type.
             */
            INT64 = 3,

            /**
             * @brief 32bit floating point number type.
             */
            FLOAT32 = 4,

            /**
             * @brief 64bit floating point number type.
             */
            FLOAT64 = 5,

            /**
             * @brief text type.
             */
            TEXT = 6,

            /**
             * @brief date type.
             */
            DATE = 7,

            /**
             * @brief time type.
             */
            TIME = 8,

            /**
             * @brief timestamp type.
             */
            TIMESTAMP = 9,

            /**
             * @brief timetz type.
             */
            TIMETZ = 10,

            /**
             * @brief timestamptz type.
             */
            TIMESTAMPTZ = 11,

            /**
             * @brief decimal type.
             */
            DECIMAL = 12,
        };
        
        /**
         * @brief Construct a new object.
         * @param type tag for the column type
         * @param byte length for the column data
         */
        ColumnType(Type type) : type_(type) {}
        
        /**
         * @brief Copy and move constructers.
         */
        ColumnType() = default;
        ColumnType(const ColumnType&) = default;
        ColumnType& operator=(const ColumnType&) = default;
        ColumnType(ColumnType&&) = default;
        ColumnType& operator=(ColumnType&&) = default;

        /**
         * @brief destructs this object.
         */
        ~ColumnType() noexcept = default;

        /**
         * @brief get type for this column.
         * @return Type of this column
         */
        ColumnType::Type get_type() const { return type_; }
    
    private:
        Type type_{};
    };

    /**
     * @brief Container to store type data for the columns.
     */
    using SetOfTypeData = std::vector<ColumnType>;
    
    /**
     * @brief Construct a new object.
     */
    Metadata() = default;

    /**
     * @brief destructs this object.
     */
    ~Metadata() noexcept = default;

    /**
     * @brief get a set of type data for this result set.
     * @param columns returns the type data
     * @return error code defined in error_code.h
     */
    const SetOfTypeData& get_types() const noexcept { return columns_; }

    /**
     * @brief push a column type.
     * @param t the type of the column
     */
    void push(ColumnType::Type t) { columns_.emplace_back(ColumnType(t)); } 

    /**
     * @brief clear this metadata.
     */
    void clear() { columns_.clear(); }

private:
    SetOfTypeData columns_;
};

using date_type = takatori::datetime::date;
using time_type = takatori::datetime::time_of_day;
using timestamp_type = takatori::datetime::time_point;
using timetz_type = std::pair<takatori::datetime::time_of_day, std::int32_t>;
using timestamptz_type = std::pair<takatori::datetime::time_point, std::int32_t>;
using decimal_type = takatori::decimal::triple;

}  // namespace ogawayama::stub
