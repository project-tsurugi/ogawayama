/*
 * Copyright 2019-2021 tsurugi project.
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

#include <vector>
#include <optional>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <ogawayama/logging.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>

#include "worker.h"

namespace ogawayama::bridge {

DEFINE_bool(remove_shm, true, "remove the shared memory prior to the execution");  // NOLINT

Worker::Worker(jogasaki::api::database& db, std::string& dbname, std::string_view shm_name, std::size_t id) : db_(db), id_(id), dbname_(dbname)
{
    std::string name{shm_name};
    name += "-" + std::to_string(id);
    shm4_connection_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_CONNECTION);
    channel_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::channel, shm4_connection_.get());
    parameters_ = std::make_unique<ogawayama::common::ParameterSet>(ogawayama::common::param::prepared, shm4_connection_.get());
    shm4_row_queue_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_ROW_QUEUE);
    channel_->send_ack(ERROR_CODE::OK);
}

void Worker::run()
{
    while(true) {
        ogawayama::common::CommandMessage::Type type;
        std::size_t ivalue;
        std::string_view string;
        try {
            if (auto rc = channel_->recv_req(type, ivalue, string); rc != ERROR_CODE::OK) {
                LOG(WARNING) << "error (" << error_name(rc) << ") occured in command receive, exiting";
                return;
            }
            VLOG(log_debug) << "--> " << ogawayama::common::type_name(type) << " : ivalue = " << ivalue << " : string = " << " \"" << string << "\"";
        } catch (std::exception &ex) {
            LOG(WARNING) << "exception in command receive, exiting";
            return;
        }

        switch (type) {
        case ogawayama::common::CommandMessage::Type::EXECUTE_STATEMENT:
            execute_statement(string);
            break;
        case ogawayama::common::CommandMessage::Type::EXECUTE_QUERY:
            if(execute_query(string, ivalue)) {
                next(ivalue);
            }
            break;
        case ogawayama::common::CommandMessage::Type::NEXT:
            channel_->send_ack(ERROR_CODE::OK);
            VLOG(log_debug) << "<-- OK";
            next(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::COMMIT:
            if (!transaction_handle_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
                VLOG(log_debug) << "<-- NO_TRANSACTION";
                break;
            }
            transaction_handle_.commit();
            channel_->send_ack(ERROR_CODE::OK);
            VLOG(log_debug) << "<-- OK";
            clear_transaction();
            break;
        case ogawayama::common::CommandMessage::Type::ROLLBACK:
            if (!transaction_handle_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
            }
            transaction_handle_.abort();
            channel_->send_ack(ERROR_CODE::OK);
            VLOG(log_debug) << "<-- OK";
            clear_transaction();
            break;
        case ogawayama::common::CommandMessage::Type::PREPARE:
            prepare(string, ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::EXECUTE_PREPARED_STATEMENT:
            execute_prepared_statement(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::EXECUTE_PREPARED_QUERY:
            {
                std::size_t rid = std::stoi(std::string(string));
                if(execute_prepared_query(ivalue, rid)) {
                    next(rid);
                }
            }
            break;
        case ogawayama::common::CommandMessage::Type::CREATE_TABLE:
            deploy_metadata(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::DROP_TABLE:
            withdraw_metadata(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::BEGIN_DDL:
            begin_ddl();
            break;
        case ogawayama::common::CommandMessage::Type::END_DDL:
            end_ddl();
            break;
        case ogawayama::common::CommandMessage::Type::DISCONNECT:
            if (transaction_handle_) {
                transaction_handle_.abort();
            }
            clear_all();
            channel_->bye_and_notify();
            VLOG(log_debug) << "<-- bye_and_notify()";
            return;
        default:
            LOG(ERROR) << "recieved an illegal command message";
            return;
        }
    }
}

void Worker::execute_statement(std::string_view sql)
{
    if (!transaction_handle_) {
        if(auto rc = db_.create_transaction(transaction_handle_); rc != jogasaki::status::ok) {
            LOG(ERROR) << "fail to db_.create_transaction(transaction_handle_)";
            channel_->send_ack(ERROR_CODE::UNKNOWN);
            VLOG(log_debug) << "<-- UNKNOWN";
            return;
        }
    }

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.create_executable(sql, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }
    if(auto rc = transaction_handle_.execute(*e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);
    VLOG(log_debug) << "<-- OK";
    return;
}

void Worker::send_metadata(std::size_t rid)
{
    auto metadata = cursors_.at(rid).result_set_->meta();
    std::size_t n = metadata->field_count();
    for (std::size_t i = 0; i < n; i++) {
        switch(metadata->at(i).kind()) {
        case jogasaki::api::field_type_kind::int4:
            cursors_.at(rid).row_queue_->push_type(TYPE::INT32, 4);
            break;
        case jogasaki::api::field_type_kind::int8:
            cursors_.at(rid).row_queue_->push_type(TYPE::INT64, 8);
            break;
        case jogasaki::api::field_type_kind::float4:
            cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT32, 4);
            break;
        case jogasaki::api::field_type_kind::float8:
            cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT64, 8);
            break;
        case jogasaki::api::field_type_kind::character:
            cursors_.at(rid).row_queue_->push_type(TYPE::TEXT, INT32_MAX);
            break;
        default:
            LOG(ERROR) << "unsurpported data type: " << metadata->at(i).kind();
            break;
        }
    }
}

bool Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_handle_) {
        if(auto rc = db_.create_transaction(transaction_handle_); rc != jogasaki::status::ok) {
            LOG(ERROR) << "fail to db_.create_transaction(transaction_handle_)";
            return false;
        }
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    
    cursor.row_queue_ = std::make_unique<ogawayama::common::RowQueue>
        (shm4_row_queue_->shm_name(ogawayama::common::param::resultset, rid), shm4_row_queue_.get());
    cursor.row_queue_->clear();

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.create_executable(sql, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return false;
    }
    auto& rs =  cursor.result_set_;
    if(auto rc = transaction_handle_.execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return false;
    }
    cursor.result_set_iterator_ = rs->iterator();

    send_metadata(rid);
    channel_->send_ack(ERROR_CODE::OK);
    VLOG(log_debug) << "<-- OK";
    return true;
}

void Worker::next(std::size_t rid)
{
    auto& cursor = cursors_.at(rid);
    auto& rq = cursor.row_queue_;
    auto& iterator = cursor.result_set_iterator_;

    if (!iterator || rq->is_closed()) {
        return;
    }

    std::size_t limit = rq->get_requested();
    for(std::size_t i = 0; i < limit; i++) {
        auto record = iterator->next();
        if (record != nullptr) {
            rq->resize_writing_row(rq->get_metadata_ptr()->get_types().size());
            for (auto& t: rq->get_metadata_ptr()->get_types()) {
                std::size_t cindex = rq->get_cindex();
                if (record->is_null(cindex)) {
                    rq->put_next_column(std::monostate());
                } else {
                    switch (t.get_type()) {
                    case TYPE::INT32:
                        rq->put_next_column(record->get_int4(cindex)); break;
                    case TYPE::INT64:
                        rq->put_next_column(record->get_int8(cindex)); break;
                    case TYPE::FLOAT32:
                        rq->put_next_column(record->get_float4(cindex)); break;
                    case TYPE::FLOAT64:
                        rq->put_next_column(record->get_float8(cindex)); break;
                    case TYPE::TEXT:
                        rq->put_next_column(record->get_character(cindex)); break;
                    case TYPE::NULL_VALUE:
                        LOG(ERROR) << "NULL_VALUE type should not be used"; break;
                    }
                }
            }
            rq->push_writing_row();
        } else {
            rq->push_writing_row();
            cursor.clear();
            rq->set_eor();
            break;
        }
    }
}

void Worker::prepare(std::string_view sql, std::size_t sid)
{
    if (prepared_statements_.size() < (sid + 1)) {
        prepared_statements_.resize(sid + 1);
    }

    if(auto rc = db_.prepare(sql, prepared_statements_.at(sid)); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);    
    VLOG(log_debug) << "<-- OK";
}

void Worker::set_params(std::unique_ptr<jogasaki::api::parameter_set>& p)
{
    auto& params = parameters_->get_params();
    for(auto& param: params) {
        std::size_t idx = &param - &params[0];
        std::visit([&p, &idx](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                std::string prefix = "p";
                if constexpr (std::is_same_v<T, std::int32_t>) {
                        p->set_int4(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, std::int64_t>) {
                        p->set_int8(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, float>) {
                        p->set_float4(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, double>) {
                        p->set_float8(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, ogawayama::common::ShmString>) {
                        p->set_character(prefix + std::to_string(idx+1), std::string_view(arg));
                    }
                else if constexpr (std::is_same_v<T, std::monostate>) {
                        p->set_null(prefix + std::to_string(idx+1));
                    }
                else {
                    LOG(ERROR) << "the type of parameter received cannot be handled";
                }
            }, param);
    }
}

void Worker::execute_prepared_statement(std::size_t sid)
{
    if (sid >= prepared_statements_.size()) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }

    if (!transaction_handle_) {
        if(auto rc = db_.create_transaction(transaction_handle_); rc != jogasaki::status::ok) {
            LOG(ERROR) << "fail to db_.create_transaction(transaction_handle_)";
            channel_->send_ack(ERROR_CODE::UNKNOWN);
            VLOG(log_debug) << "<-- UNKNOWN";
            return;
        }
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.resolve(cursors_.at(sid).prepared_, std::shared_ptr{std::move(params)}, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }
    if(auto rc = transaction_handle_.execute(*e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);
    VLOG(log_debug) << "<-- OK";
}

bool Worker::execute_prepared_query(std::size_t sid, std::size_t rid)
{
    if (sid >= prepared_statements_.size()) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return false;
    }

    if (!transaction_handle_) {
        if(auto rc = db_.create_transaction(transaction_handle_); rc != jogasaki::status::ok) {
            LOG(ERROR) << "fail to db_.create_transaction(transaction_handle_)";
            return false;
        }
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);

    cursor.row_queue_ = std::make_unique<ogawayama::common::RowQueue>
        (shm4_row_queue_->shm_name(ogawayama::common::param::resultset, rid), shm4_row_queue_.get());
    cursor.row_queue_->clear();

    auto params = jogasaki::api::create_parameter_set();
    set_params(params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.resolve(cursors_.at(sid).prepared_, std::shared_ptr{std::move(params)}, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return false;
    }
    auto& rs =  cursor.result_set_;
    if(auto rc = transaction_handle_.execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
        return false;
    }
    cursor.result_set_iterator_ = rs->iterator();

    send_metadata(rid);
    channel_->send_ack(ERROR_CODE::OK);
    VLOG(log_debug) << "<-- OK";
    return true;
}

void Worker::begin_ddl()
{
    ERROR_CODE err_code = ERROR_CODE::OK;

    if (!transaction_handle_) {
        if(auto rc = db_.create_transaction(transaction_handle_); rc != jogasaki::status::ok) {  // FIXME use transaction option to specify exclusive exexution
            LOG(ERROR) << "fail to db_.create_transaction(transaction_handle_) " << jogasaki::to_string_view(rc);
            err_code = ERROR_CODE::UNKNOWN;
        }
    } else {
        err_code = ERROR_CODE::TRANSACTION_ALREADY_STARTED;
    }
    channel_->send_ack(err_code);
    VLOG(log_debug) << "<-- " << ogawayama::stub::error_name(err_code);
}

void Worker::end_ddl()
{
    ERROR_CODE err_code = ERROR_CODE::OK;

    if (!transaction_handle_) {
        err_code = ERROR_CODE::NO_TRANSACTION;
    } else {
        transaction_handle_.commit();
        clear_transaction();
    }
    channel_->send_ack(err_code);
    VLOG(log_debug) << "<-- " << ogawayama::stub::error_name(err_code);
}

void Worker::deploy_metadata(std::size_t table_id)
{
    manager::metadata::ErrorCode error;

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(dbname_);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        VLOG(log_debug) << "<-- FILE_IO_ERROR";
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(dbname_);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        VLOG(log_debug) << "<-- FILE_IO_ERROR";
        return;
    }

    boost::property_tree::ptree table;
    if (error = tables->get(table_id, table); error == manager::metadata::ErrorCode::OK) {
        if (FLAGS_v >= log_debug) {
            std::ostringstream oss;
            boost::property_tree::json_parser::write_json(oss, table);
            VLOG(log_debug) << "==== schema metadata begin ====";
            VLOG(log_debug) << oss.str();
            VLOG(log_debug) << "==== schema metadata end ====";
        }

        VLOG(log_debug) << "found table with id " << table_id ;
        // table metadata
        auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
        auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!id || !table_name || (id.value() != table_id)) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            VLOG(log_debug) << "<-- INVALID_PARAMETER";
            return;
        }

        VLOG(log_debug) << " name is " << table_name.value();

        std::vector<std::size_t> pk_columns;  // index of primary key column: received order, content: column_number
        boost::optional<std::string> pk_columns_name;
        std::size_t unique_columns_n = 0;
        std::vector<std::vector<std::size_t>> unique_columns;  // index of unique constraint: received order, content: column_number
        std::vector<boost::optional<std::string>> unique_columns_name;
        BOOST_FOREACH (const auto& constraint_node, table.get_child(manager::metadata::Tables::CONSTRAINTS_NODE)) {
            auto& constraint = constraint_node.second;
            auto constraint_type = constraint.get_optional<int64_t>(manager::metadata::Constraint::TYPE);
            // TypeがPRIMARY_KEYか否か
            auto type_value = static_cast<manager::metadata::Constraint::ConstraintType>(constraint_type.value());
            switch (type_value) {
            case manager::metadata::Constraint::ConstraintType::PRIMARY_KEY:
                // Primary Keyが設定されたカラムメタデータのカラム番号と名前を読み込む
                BOOST_FOREACH (const auto& column_node, constraint.get_child(manager::metadata::Constraint::COLUMNS)) {
                    auto& constraint_column = column_node.second;
                    auto column = constraint_column.get_value_optional<int64_t>();
                    pk_columns.emplace_back(column.value());
                }
                pk_columns_name = constraint.get_optional<std::string>("name");
                break;
            case manager::metadata::Constraint::ConstraintType::UNIQUE:
                // Unique制約が設定されたカラムメタデータのカラム番号と名前を読み込む
                unique_columns_name.resize(unique_columns_n + 1);
                unique_columns_name.at(unique_columns_n) = constraint.get_optional<std::string>("name");
                unique_columns.resize(unique_columns_n + 1);
                auto& v = unique_columns.at(unique_columns_n);
                unique_columns_n++;
                BOOST_FOREACH (const auto& column_node, constraint.get_child(manager::metadata::Constraint::COLUMNS)) {
                    auto& constraint_column = column_node.second;
                    auto column = constraint_column.get_value_optional<int64_t>();
                    v.emplace_back(column.value());
                }
                break;
            }
        }

        // column metadata
        std::map<std::size_t, const boost::property_tree::ptree*> columns_map;          // key: Column.ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, table.get_child(manager::metadata::Tables::COLUMNS_NODE)) {
            auto column_number = node.second.get_optional<uint64_t>(manager::metadata::Column::COLUMN_NUMBER);
            if (!column_number) {
                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return;
            }
            columns_map[column_number.value()] = &node.second;
            VLOG(log_debug) << " found column metadata, column_number " << column_number.value();
        }

        takatori::util::reference_vector<yugawara::storage::column> columns;            // index: ordinalPosition order (the first value of index and order are different)
        std::vector<std::size_t> value_columns;                                         // index: received order, content: ordinalPosition of the column
        std::vector<bool> is_descendant;                                                // index: ordinalPosition order (the first value of index and order are different)
        std::size_t column_number_value = 1;
        for(auto &&e : columns_map) {
            if (column_number_value != e.first) {
                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return;
            }
            const boost::property_tree::ptree& column = *e.second;
            
            auto is_not_null = column.get_optional<bool>(manager::metadata::Column::IS_NOT_NULL);
            auto data_type_id = column.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Column::DATA_TYPE_ID);
            auto name = column.get_optional<std::string>(manager::metadata::Column::NAME);
            auto column_number = column.get_optional<uint64_t>(manager::metadata::Column::COLUMN_NUMBER);
            if (!is_not_null || !data_type_id || !name) {
                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return;
            }
            auto is_not_null_value = is_not_null.value();
            auto data_type_id_value = static_cast<manager::metadata::DataTypes::DataTypesId>(data_type_id.value());
            auto name_value = name.value();

            if (std::vector<std::size_t>::iterator itr = std::find(pk_columns.begin(), pk_columns.end(), column_number_value); itr != pk_columns.end()) {  // is this pk_column ?
                if(!is_not_null_value) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // pk_column must be is_not_null
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return;
                }
            } else {  // this is value column
                value_columns.emplace_back(column_number_value);
            }
            VLOG(log_debug) << " found column, name : data_type_id : is_not_null " << name_value << " : " << static_cast<std::size_t>(data_type_id_value) <<  " : " << (is_not_null_value ? "not_null" : "nullable");

            switch(data_type_id_value) {  // build yugawara::storage::column
            case manager::metadata::DataTypes::DataTypesId::INT32:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int4(), yugawara::variable::nullity(!is_not_null_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::INT64:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int8(), yugawara::variable::nullity(!is_not_null_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float4(), yugawara::variable::nullity(!is_not_null_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float8(), yugawara::variable::nullity(!is_not_null_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
            {
                std::size_t data_length_value{1};  // for CHAR
                auto varying = column.get_optional<bool>(manager::metadata::Column::VARYING);
                if(!varying) {  // varying field is necessary for CHAR/VARCHRA
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return;
                }
                auto varying_value = varying.value();
                if((!varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::CHAR)) ||
                   (varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::VARCHAR))) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return;
                }

                std::vector<boost::optional<int64_t>> data_length_vector;
                boost::property_tree::ptree data_length_array = column.get_child(manager::metadata::Column::DATA_LENGTH);
                BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, data_length_array) {
                    const boost::property_tree::ptree& value = node.second;
                    data_length_vector.emplace_back(value.get_value_optional<int64_t>());
                }
                auto data_length = data_length_vector.at(0);
                if (!data_length) {
                    if(varying_value) {  // data_length field is necessary for VARCHAR
                        channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                        VLOG(log_debug) << "<-- UNSUPPORTED";
                        return;
                    }
                } else {
                    data_length_value = data_length.value();
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                                    takatori::type::character(takatori::type::varying_t(varying_value), data_length_value),
                                                                    yugawara::variable::nullity(!is_not_null_value)));
                break;
            }
            default:
                std::abort();  // FIXME
            }
            column_number_value++;
        }

        auto t = std::make_shared<yugawara::storage::table>(
            std::make_optional(static_cast<yugawara::storage::table::definition_id_type>(table_id)),
            yugawara::storage::table::simple_name_type(table_name.value()),
            std::move(columns)
        );
        if (auto rc = db_.create_table(t); rc != jogasaki::status::ok) {
            channel_->send_ack((rc == jogasaki::status::err_already_exists) ? ERROR_CODE::INVALID_PARAMETER : ERROR_CODE::UNKNOWN);
            VLOG(log_debug) << "<-- " << ((rc == jogasaki::status::err_already_exists) ? "INVALID_PARAMETER" : "UNKNOWN");
            return;
        }
        
        // build key metadata (yugawara::storage::index::key)
        std::vector<yugawara::storage::index::key> keys;
        for (std::size_t position : pk_columns) {
            keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1]));
        }

        // build value metadata (yugawara::storage::index::column_ref)
        std::vector<yugawara::storage::index::column_ref> values;
        for(std::size_t position : value_columns) {
            values.emplace_back(yugawara::storage::index::column_ref(t->columns()[position-1]));
        }
        std::string pn{};
        if (pk_columns_name) {
            pn = pk_columns_name.value();
        }
        if (pn.empty()) {
            pn = table_name.value();
        }

        auto i = std::make_shared<yugawara::storage::index>(
            std::make_optional(static_cast<yugawara::storage::index::definition_id_type>(table_id)),
            t,
            yugawara::storage::index::simple_name_type(pn),
            std::move(keys),
            std::move(values),
            yugawara::storage::index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            }
        );
        if(db_.create_index(i) != jogasaki::status::ok) {
            channel_->send_ack(ERROR_CODE::UNKNOWN);
            VLOG(log_debug) << "<-- UNKNOWN";
            return;
        }

        // secondary index from unique constraint
        for (std::size_t i = 0; i < unique_columns_n; i++) {
            auto& secondary_index = unique_columns.at(i);
            auto& secondary_index_name = unique_columns_name.at(i);

            // build key metadata (yugawara::storage::index::key)
            std::vector<yugawara::storage::index::key> si_keys;
            si_keys.clear();
            for (std::size_t position : secondary_index) {
                si_keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1]));
            }
            std::string in{};
            if (secondary_index_name) {
                in = secondary_index_name.value();
            }
            if (in.empty()) {
                in = table_name.value();
                in += "_constraint_";
                in += std::to_string(i);
            }

            auto si = std::make_shared<yugawara::storage::index>(
                std::make_optional(static_cast<yugawara::storage::index::definition_id_type>(table_id)),
                t,
                yugawara::storage::index::simple_name_type(in),
                std::move(si_keys),
                std::move(std::vector<yugawara::storage::index::column_ref>{}),
                yugawara::storage::index_feature_set{
                    ::yugawara::storage::index_feature::find,
                    ::yugawara::storage::index_feature::scan,
                    ::yugawara::storage::index_feature::unique,
                }
            );
            if(db_.create_index(si) != jogasaki::status::ok) {
                channel_->send_ack(ERROR_CODE::UNKNOWN);
                VLOG(log_debug) << "<-- UNKNOWN";
                return;
            }
        }

        channel_->send_ack(ERROR_CODE::OK);
        VLOG(log_debug) << "<-- OK";
    } else {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
    }
}

void Worker::withdraw_metadata(std::size_t table_id)
{
    manager::metadata::ErrorCode error;

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(dbname_);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        VLOG(log_debug) << "<-- FILE_IO_ERROR";
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(dbname_);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        VLOG(log_debug) << "<-- FILE_IO_ERROR";
        return;
    }

    boost::property_tree::ptree table;
    if ((error = tables->get(table_id, table)) == manager::metadata::ErrorCode::OK) {
        VLOG(log_debug) << "found table with id " << table_id ;
        // table metadata
        auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
        auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!id || !table_name || (id.value() != table_id)) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            VLOG(log_debug) << "<-- INVALID_PARAMETER";
            return;
        }

        VLOG(log_debug) << " name is " << table_name.value();
        if (auto rc = db_.drop_table(table_name.value()); rc != jogasaki::status::ok) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }
        if (auto rc = db_.drop_index(table_name.value()); rc != jogasaki::status::ok) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }
        channel_->send_ack(ERROR_CODE::OK);
        VLOG(log_debug) << "<-- OK";
    } else {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        VLOG(log_debug) << "<-- UNKNOWN";
    }
}

}  // ogawayama::bridge
