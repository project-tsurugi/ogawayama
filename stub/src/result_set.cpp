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

#include "result_setImpl.h"

namespace ogawayama::stub {

ResultSet::Impl::Impl(ResultSet *result_set) : envelope_(result_set) {}
    //    row_queue_(std::make_unique<ogawayama::common::RowQueue>("name", MEMORY_ALLOCATOR)) {}
    
/**
 * @brief connect to the DB and get Result_Set class
 * @param result_set returns a result_set class
 * @return true in error, otherwise false
 */
ErrorCode ResultSet::Impl::next()
{
    row_queue_->next();
    index_ = 0;
    return ErrorCode::OK;
}

/**
 * @brief get value in integer from the current row.
 * @param index culumn number, begins from one
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<typename T>
ErrorCode ResultSet::Impl::next_column(T &value) {
    ogawayama::common::ShmRow r = row_queue_->get_current_row();

    if (r.size() == 0) {
        return ErrorCode::END_OF_ROW;
    }
    
    ogawayama::common::ShmColumn c = r.at(index_);
    try {
        value = std::get<T>(c);
        return ErrorCode::OK;
    }
    catch (const std::bad_variant_access&) {
        if (std::holds_alternative<std::monostate>(c)) {
            return ErrorCode::COLUMN_WAS_NULL;
        }
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief constructor of ResultSet class
 */
ResultSet::ResultSet(Transaction *transaction)
    : impl_(std::make_unique<ResultSet::Impl>(this)), transaction_(transaction) {}

ErrorCode ResultSet::next() { return impl_->next(); }

template<typename T>
ErrorCode ResultSet::next_column(T &value) { return impl_->next_column<T>(value); }

}  // namespace ogawayama::stub
