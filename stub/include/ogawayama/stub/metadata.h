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
#ifndef STUB_METADATA_H_
#define STUB_METADATA_H_

#include <cstddef>
#include <vector>

#include "ogawayama/stub/error_code.h"

namespace ogawayama::stub {

namespace type {}

/**
 * @brief Provides semantic information of a ResultSet.
 */
class Metadata {
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
             * @brief 16bit integral number type.
             */
            INT16,

            /**
             * @brief 32bit integral number type.
             */
            INT32,

            /**
             * @brief 64bit integral number type.
             */
            INT64,

            /**
             * @brief 32bit floating point number type.
             */
            FLOAT32,

            /**
             * @brief 64bit floating point number type.
             */
            FLOAT64,

            /**
             * @brief text type.
             */
            TEXT,
        };
        
        /**
         * @brief Construct a new object.
         * @param type tag for the column type
         * @param byte length for the column data
         */
        ColumnType(Type type, std::size_t size)
            : type_(type), size_(size)
        {}

        /**
         * @brief destructs this object.
         */
        ~ColumnType() noexcept = default;

        /**
         * @brief get type for this column.
         * @param type returns the type
         * @return error code defined in error_code.h
         */
        ErrorCode get_type(Metadata::ColumnType::Type &type);
    
    private:
        std::size_t size_;
        Type type_;
    };

public:
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
    
private:
    SetOfTypeData columns_;
};

}  // namespace ogawayama::stub

#endif  // STUB_METADATA_H_
