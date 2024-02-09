/*
 * Copyright 2019-2023 Project Tsurugi.
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

#include <jogasaki/serializer/value_input.h>

#include "stubImpl.h"
#include "transactionImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of ResultSet::Impl class
 */
class ResultSet::Impl
{
public:
    Impl(Transaction::Impl*, std::unique_ptr<tateyama::common::wire::session_wire_container::resultset_wires_container>, ::jogasaki::proto::sql::response::ResultSetMetadata, std::size_t query_index);
    ~Impl();

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;
    Impl(Impl&&) = delete;
    Impl& operator=(Impl&&) = delete;

    ErrorCode get_metadata(MetadataPtr &);
    ErrorCode next();
    template<typename T>
        ErrorCode next_column(T &value);

 private:
    Transaction::Impl* manager_;
    std::unique_ptr<tateyama::common::wire::session_wire_container::resultset_wires_container> resultset_wire_;
    ::jogasaki::proto::sql::response::ResultSetMetadata metadata_;
    std::size_t column_number_;
    std::size_t query_index_;

    ogawayama::stub::Metadata ogawayama_metadata_{};
    bool ogawayama_metadata_valid_{false};

    jogasaki::serializer::buffer_view buf_{};
    jogasaki::serializer::buffer_view::const_iterator iter_{};

    std::size_t c_idx_{0};

    ErrorCode next_column_common() {
        if (c_idx_++ < column_number_) {
            return ErrorCode::OK;
        }
        resultset_wire_->dispose();
        c_idx_ = 0;
        return ErrorCode::END_OF_COLUMN;
    }
};

}  // namespace ogawayama::stub
