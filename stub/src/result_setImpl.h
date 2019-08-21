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
    Impl(ResultSet *);
    ErrorCode next();
    template<typename T>
        ErrorCode next_column(T &value);
 private:
    std::size_t index_ {};
    std::unique_ptr<ogawayama::common::RowQueue> row_queue_;
    ResultSet *envelope_;
};

}  // namespace ogawayama::stub

#endif  // RESULT_SETIMPL_H_
