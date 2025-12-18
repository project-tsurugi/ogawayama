/*
 * Copyright 2019-2024 Project Tsurugi.
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

#include <iostream>
#include <exception>

#include "result_setImpl.h"

namespace ogawayama::stub {

ResultSet::Impl::Impl(Transaction::Impl* manager, std::unique_ptr<tateyama::common::wire::session_wire_container::resultset_wires_container> resultset_wire, ::jogasaki::proto::sql::response::ResultSetMetadata metadata, std::size_t query_index)
    : manager_(manager),
      resultset_wire_(std::move(resultset_wire)),
      metadata_(std::move(metadata)),
      column_number_(metadata_.columns_size()),
      query_index_(query_index)
{
}

ResultSet::Impl::~Impl() {
    try {
        if (resultset_wire_) {
            resultset_wire_->set_closed();
            manager_->receive_body(query_index_);
        }
    } catch (std::exception &ex) {
        std::cerr << ex.what() << std::endl;
    }
}

ErrorCode ResultSet::Impl::close() {
    try {
        resultset_wire_->set_closed();
        resultset_wire_.reset();
        manager_->receive_body(query_index_);
        return ErrorCode::END_OF_ROW;
    } catch (std::runtime_error &ex) {
        return ErrorCode::SERVER_ERROR;
    }
}

/**
 * @brief get metadata for the result set.
 * @param metadata returns a MetadataPtr pointing to the metadata class
 * @return error code defined in error_code.h
 */
ErrorCode ResultSet::Impl::get_metadata(MetadataPtr& metadata)
{
    if (!ogawayama_metadata_valid_) {
        ogawayama_metadata_.clear();
        for (int j = 0; j < column_number_; j++) {
            const ::jogasaki::proto::sql::common::Column& column = metadata_.columns(j);
            switch(column.type_info_case()) {
            case ::jogasaki::proto::sql::common::Column::TypeInfoCase::kAtomType:
                switch(column.atom_type()) {
                case ::jogasaki::proto::sql::common::AtomType::INT4: ogawayama_metadata_.push(Metadata::ColumnType::Type::INT32); break;
                case ::jogasaki::proto::sql::common::AtomType::INT8: ogawayama_metadata_.push(Metadata::ColumnType::Type::INT64); break;
                case ::jogasaki::proto::sql::common::AtomType::FLOAT4: ogawayama_metadata_.push(Metadata::ColumnType::Type::FLOAT32); break;
                case ::jogasaki::proto::sql::common::AtomType::FLOAT8: ogawayama_metadata_.push(Metadata::ColumnType::Type::FLOAT64); break;
                case ::jogasaki::proto::sql::common::AtomType::DECIMAL: ogawayama_metadata_.push(Metadata::ColumnType::Type::DECIMAL); break;
                case ::jogasaki::proto::sql::common::AtomType::CHARACTER: ogawayama_metadata_.push(Metadata::ColumnType::Type::TEXT); break;
                case ::jogasaki::proto::sql::common::AtomType::OCTET: ogawayama_metadata_.push(Metadata::ColumnType::Type::OCTET); break;
                case ::jogasaki::proto::sql::common::AtomType::BIT: break;
                case ::jogasaki::proto::sql::common::AtomType::DATE: ogawayama_metadata_.push(Metadata::ColumnType::Type::DATE); break;
                case ::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY: ogawayama_metadata_.push(Metadata::ColumnType::Type::TIME); break;
                case ::jogasaki::proto::sql::common::AtomType::TIME_POINT: ogawayama_metadata_.push(Metadata::ColumnType::Type::TIMESTAMP); break;
                case ::jogasaki::proto::sql::common::AtomType::DATETIME_INTERVAL: break;
                case ::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE: ogawayama_metadata_.push(Metadata::ColumnType::Type::TIMETZ); break;
                case ::jogasaki::proto::sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE: ogawayama_metadata_.push(Metadata::ColumnType::Type::TIMESTAMPTZ); break;
                case ::jogasaki::proto::sql::common::AtomType::CLOB: break;
                case ::jogasaki::proto::sql::common::AtomType::BLOB: break;
                case ::jogasaki::proto::sql::common::AtomType::UNKNOWN: break;
                default: break;
                }
                break;
            case ::jogasaki::proto::sql::common::Column::TypeInfoCase::kRowType: break;
            case ::jogasaki::proto::sql::common::Column::TypeInfoCase::kUserType: break;
            case ::jogasaki::proto::sql::common::Column::TypeInfoCase::TYPE_INFO_NOT_SET: break;
            }
        }
        ogawayama_metadata_valid_ = true;
    }
    metadata = &ogawayama_metadata_;
    return ErrorCode::OK;
}

/**
 * @brief move to next row
 * @return error code defined in error_code.h
 */
ErrorCode ResultSet::Impl::next()
{
    if (c_idx_ != 0) {
        resultset_wire_->dispose();
        c_idx_ = 0;
    }
    auto record = resultset_wire_->get_chunk();
    if (record.empty()) {
        return close();
    }
    buf_ = jogasaki::serializer::buffer_view(const_cast<char*>(record.data()), record.size());
    iter_ = buf_.begin();
    jogasaki::serializer::read_row_begin(iter_, buf_.end());
    return ErrorCode::OK;
}

/**
 * @brief get int64 value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(std::int64_t& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::int_:
        value = jogasaki::serializer::read_int(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: int_ expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief get float64 value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(double& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::float8:
        value = jogasaki::serializer::read_float8(iter_, buf_.end());
        return ErrorCode::OK;
    case jogasaki::serializer::entry_type::float4:
        value = jogasaki::serializer::read_float4(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: float4 or float8 expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

template<>
ErrorCode ResultSet::Impl::next_column(std::int16_t& value) {
    std::int64_t v{};
    auto rv = next_column(v);
    value = static_cast<std::int16_t>(v);
    return rv;
}
template<>
ErrorCode ResultSet::Impl::next_column(std::int32_t& value) {
    std::int64_t v{};
    auto rv = next_column(v);
    value = static_cast<std::int32_t>(v);
    return rv;
}
template<>
ErrorCode ResultSet::Impl::next_column(float& value) {
    double v{};
    auto rv = next_column(v);
    value = static_cast<float>(v);
    return rv;
}

/**
 * @brief get text value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(std::string_view& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::character:
        value = jogasaki::serializer::read_character(iter_, buf_.end());
        return ErrorCode::OK;
    case jogasaki::serializer::entry_type::octet:
        value = jogasaki::serializer::read_octet(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: character expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}
template<>
ErrorCode ResultSet::Impl::next_column(std::string& value) {
    std::string_view sv;
    ErrorCode err = next_column(sv);
    if( err == ErrorCode::OK) {
        if (value.capacity() < (sv.length() + 1)) {
            value.reserve(sv.length() + 1);
        }
        value = sv;
    }
    return err;
}


/**
 * @brief get date value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(takatori::datetime::date& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::date:
        value = jogasaki::serializer::read_date(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: date expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief get time_of_day value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(takatori::datetime::time_of_day& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::time_of_day:
        value = jogasaki::serializer::read_time_of_day(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: time_of_day expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief get time_point value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(takatori::datetime::time_point& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::time_point:
        value = jogasaki::serializer::read_time_point(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: time_point expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
  @brief get time_of_day_with_offset value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(std::pair<takatori::datetime::time_of_day, std::int32_t>& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::time_of_day_with_offset:
        value = jogasaki::serializer::read_time_of_day_with_offset(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: time_of_day_with_offset expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief get time_point_with_offset value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(std::pair<takatori::datetime::time_point, std::int32_t>& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::time_point_with_offset:
        value = jogasaki::serializer::read_time_point_with_offset(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: time_point_with_offset expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}

/**
 * @brief get time_point_with_offset value from the current row.
 * @param value returns the value
 * @return error code defined in error_code.h
 */
template<>
ErrorCode ResultSet::Impl::next_column(takatori::decimal::triple& value) {
    if (auto rv = next_column_common(); rv != ErrorCode::OK) {
        return rv;
    }
    switch (auto entry_type = jogasaki::serializer::peek_type(iter_, buf_.end())) {
    case jogasaki::serializer::entry_type::end_of_contents:
        jogasaki::serializer::read_end_of_contents(iter_, buf_.end());
        return ErrorCode::END_OF_ROW;
    case jogasaki::serializer::entry_type::null:
        jogasaki::serializer::read_null(iter_, buf_.end());
        return ErrorCode::COLUMN_WAS_NULL;
    case jogasaki::serializer::entry_type::decimal:
    case jogasaki::serializer::entry_type::int_:
        value = jogasaki::serializer::read_decimal(iter_, buf_.end());
        return ErrorCode::OK;
    default:
        std::cerr << "error: decimal expected, actually " << jogasaki::serializer::to_string_view(entry_type) << " received" << std::endl;
        return ErrorCode::COLUMN_TYPE_MISMATCH;
    }
}


/**
 * @brief constructor of ResultSet class
 */
ResultSet::ResultSet(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

/**
 * @brief destructor of ResultSet class
 */
ResultSet::~ResultSet() = default;

/**
 * @brief get metadata for the result set.
 * @param metadata returns the metadata class
 * @return error code defined in error_code.h
 */
ErrorCode ResultSet::get_metadata(MetadataPtr& metadata)
{
    return impl_->get_metadata(metadata);
}

ErrorCode ResultSet::next() { return impl_->next(); }
template<>
ErrorCode ResultSet::next_column(std::int16_t& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::int32_t& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::int64_t& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(float& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(double& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::string_view& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::string& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(takatori::datetime::date& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(takatori::datetime::time_of_day& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(takatori::datetime::time_point& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::pair<takatori::datetime::time_of_day, std::int32_t>& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(std::pair<takatori::datetime::time_point, std::int32_t>& value) { return impl_->next_column(value); }
template<>
ErrorCode ResultSet::next_column(takatori::decimal::triple& value) { return impl_->next_column(value); }

}  // namespace ogawayama::stub
