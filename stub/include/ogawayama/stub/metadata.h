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

namespace ogawayama::stub {

namespace type {}

/**
 * @brief Provides semantic information of a ResultSet.
 */
class MetaData {
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
         * @brief represents nullity of the type.
         */
        enum class Nullity : bool {

            /**
             * @brief the value is never null.
             */
            NEVER_NULL = false,

            /**
             * @brief the value may be or must be null.
             */
            NULLABLE = true,
        };

    private:
        std::size_t size_;
        Type type_;
        Nullity nullable_;

    public:
        /**
         * @brief Construct a new object.
         * @param type the column type
         */
        ColumnType(Type type, std::size_t size, Nullity nullable)
            : type_(type), size_(size), nullable_(nullable)
        {}

        /**
         * @brief destructs this object.
         */
        ~ColumnType() noexcept = default;
        
    };

private:
    std::vector<ColumnType> columns_;

};

}  // namespace ogawayama::stub

#endif  // STUB_METADATA_H_
