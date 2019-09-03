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
#ifndef RESULT_SETIMPL_H_
#define RESULT_SETIMPL_H_

#include "stubImpl.h"
#include "transactionImpl.h"
#include "ogawayama/common/row_queue.h"

namespace ogawayama::stub {

/**
 * @brief constructor of ResultSet::Impl class
 */
class ResultSet::Impl
{
public:
    Impl(ResultSet *, std::size_t);
    ErrorCode get_metadata(MetadataPtr &);
    ErrorCode next();
    template<typename T>
        ErrorCode next_column(T &value);
    auto get_id() { return id_; }
    void set_metadata() {
        const ogawayama::common::ShmSetOfTypeData & set_of_type_data_from = row_queue_->get_types();
        for(auto type: set_of_type_data_from) {
            metadata_.push(type.get_type(), type.get_length());
        }
    } // for transactionImpl
    void clear() {
        metadata_.clear();
        row_queue_->clear();
    }
    ogawayama::common::RowQueue * get_row_queue() { return row_queue_.get(); }
    
 private:
    ResultSet *envelope_;

    std::size_t id_;
    std::size_t c_idx_;
    Metadata metadata_;
    std::unique_ptr<ogawayama::common::RowQueue> row_queue_;

    friend class transactionImpl;
};

}  // namespace ogawayama::stub

#endif  // RESULT_SETIMPL_H_
