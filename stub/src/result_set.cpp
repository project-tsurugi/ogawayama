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

ResultSet::Impl::Impl(ResultSet *result_set, std::size_t id) : envelope_(result_set), id_(id), c_idx_(0)
{
    Connection *connection = envelope_->get_manager()->get_manager();
    
    row_queue_ = std::make_unique<ogawayama::common::RowQueue>
        (
         connection->get_impl()->get_shm4_row_queue()->shm_name(ogawayama::common::param::resultset, id_).c_str(),
         connection->get_impl()->get_shm4_row_queue(),
         true
         );
}
    
/**
 * @brief get metadata for the result set.
 * @param metadata returns the metadata class
 * @return error code defined in error_code.h
 */
ErrorCode ResultSet::Impl::get_metadata(MetadataPtr &metadata)
{
    metadata = row_queue_->get_metadata_ptr();
    return ErrorCode::OK;
}

/**
 * @brief connect to the DB and get Result_Set class
 * @param result_set returns a result_set class
 * @return true in error, otherwise false
 */
ErrorCode ResultSet::Impl::next()
{
    if(row_queue_->is_need_next()) {
        envelope_->get_manager()->get_impl()->get_channel()->send_req(ogawayama::common::CommandMessage::Type::NEXT, id_);
        ErrorCode reply = envelope_->get_manager()->get_impl()->get_channel()->recv_ack();
        if (reply != ErrorCode::OK) {
            return ErrorCode::SERVER_FAILURE;
        }
    }

    row_queue_->next();
    if (row_queue_->get_current_row().size() == 0) {
        return ErrorCode::END_OF_ROW;
    }
    c_idx_ = 0;
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
    const ogawayama::common::ShmRowArgs &r = row_queue_->get_current_row();

    if (c_idx_ >= r.size()) {
        return ErrorCode::END_OF_COLUMN;
    }
    ogawayama::common::ShmClmArg c = r.at(c_idx_++);
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
 * @brief get value in integer from the current row.
 * @param index culumn number, begins from one
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(std::string_view &s) {
    const ogawayama::common::ShmRowArgs &r = row_queue_->get_current_row();

    if (c_idx_ >= r.size()) {
        return ErrorCode::END_OF_COLUMN;
    }
    const ogawayama::common::ShmClmArg &c = r.at(c_idx_++);
    try {
        s = std::get<ogawayama::common::ShmString>(c);
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
 * @brief get metadata for the result set.
 * @param metadata returns the metadata class
 * @return error code defined in error_code.h
 */
ErrorCode ResultSet::get_metadata(MetadataPtr &metadata)
{
    return impl_->get_metadata(metadata);
}

/**
 * @brief constructor of ResultSet class
 */
ResultSet::ResultSet(Transaction *transaction, std::size_t id) : manager_(transaction)
{
    impl_ = std::make_unique<ResultSet::Impl>(this, id);
}

/**
 * @brief destructor of ResultSet class
 */
ResultSet::~ResultSet() = default;

ErrorCode ResultSet::next() { return impl_->next(); }

template<>
ErrorCode ResultSet::next_column(std::int16_t &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::int32_t &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::int64_t &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(float &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(double &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::string_view &value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::string &value) {
    std::string_view sv;
    ErrorCode err = impl_->next_column(sv);
    if( err == ErrorCode::OK) {
        value = sv;
    }
    return err;
}

}  // namespace ogawayama::stub
