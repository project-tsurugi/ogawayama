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

#include <iostream>
#include <vector>
#include <boost/foreach.hpp>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "shakujo/common/core/type/Int.h"
#include "shakujo/common/core/type/Float.h"
#include "shakujo/common/core/type/Char.h"

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>

#include "worker.h"

namespace ogawayama::server {

DECLARE_string(dbname);

Worker::Worker(jogasaki::api::database& db, std::size_t id) : db_(db), id_(id)
{
    std::string name = FLAGS_dbname + std::to_string(id);
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
            if (channel_->recv_req(type, ivalue, string) != ERROR_CODE::OK) {
                std::cerr << __func__ << " " << __LINE__ << ": exiting" << std::endl;
                return;
            }
            VLOG(1) << __func__ << ":" << __LINE__ << " recieved " << ivalue << " " << ogawayama::common::type_name(type) << " \"" << string << "\"";
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
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
            next(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::COMMIT:
            if (!transaction_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
                break;
            }
            transaction_->commit();
            channel_->send_ack(ERROR_CODE::OK);
            clear_transaction();
            break;
        case ogawayama::common::CommandMessage::Type::ROLLBACK:
            if (!transaction_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
            }
            transaction_->abort();
            channel_->send_ack(ERROR_CODE::OK);
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
        case ogawayama::common::CommandMessage::Type::DISCONNECT:
            if (transaction_) {
                transaction_->abort();
            }
            clear_all();
            channel_->bye_and_notify();
            return;
        default:
            std::cerr << "recieved an illegal command message" << std::endl;
            return;
        }
    }
}

void Worker::execute_statement(std::string_view sql)
{
    if (!transaction_) {
        transaction_ = db_.create_transaction();
    }

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.create_executable(sql, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);
    return;
}

void Worker::send_metadata(std::size_t rid)
{
    using namespace shakujo::common::core;

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
            std::cerr << "unsurpported data type: " << metadata->at(i).kind() << std::endl;
            break;
        }
    }
}

bool Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_.create_transaction();
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
        return false;
    }
    auto& rs =  cursor.result_set_;
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    
    send_metadata(rid);
    channel_->send_ack(ERROR_CODE::OK);
    return true;
}

void Worker::next(std::size_t rid)
{
    auto& cursor = cursors_.at(rid);
    auto& rq = cursor.row_queue_;
    auto iterator = cursor.result_set_->iterator();
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
                        std::cerr << "NULL_VALUE type should not be used" << std::endl; break;
                    }
                }
            }
            rq->push_writing_row();
        } else {
            rq->push_writing_row();
            cursor.clear();
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
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);    
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
                    std::cerr << __func__ << " " << __LINE__ << ": received a type of parameter that cannot be handled" << std::endl;
                }
            }, param);
    }
}

void Worker::execute_prepared_statement(std::size_t sid)
{
    if (sid >= prepared_statements_.size()) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }

    if (!transaction_) {
        transaction_ = db_.create_transaction();
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.resolve(*cursors_.at(sid).prepared_, *params, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }
    channel_->send_ack(ERROR_CODE::OK);
}

bool Worker::execute_prepared_query(std::size_t sid, std::size_t rid)
{
    if (sid >= prepared_statements_.size()) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }

    if (!transaction_) {
        transaction_ = db_.create_transaction();
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
    if(auto rc = db_.resolve(*cursors_.at(sid).prepared_, *params, e); rc != jogasaki::status::ok) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    auto& rs =  cursor.result_set_;
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }

    send_metadata(rid);
    channel_->send_ack(ERROR_CODE::OK);
    return true;
}

void Worker::deploy_metadata(std::size_t table_id)
{
    manager::metadata::ErrorCode error;

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(FLAGS_dbname);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(FLAGS_dbname);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }

    boost::property_tree::ptree table;
    if ((error = tables->get(table_id, table)) == manager::metadata::ErrorCode::OK) {

        // table metadata
        auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
        auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!id || !table_name || (id.value() != table_id)) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }

        boost::property_tree::ptree primary_keys = table.get_child(manager::metadata::Tables::PRIMARY_KEY_NODE);

        std::vector<std::size_t> pk_columns;  // index: received order, content: ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, primary_keys) {
            const boost::property_tree::ptree& value = node.second;
            boost::optional<uint64_t> primary_key = value.get_value_optional<uint64_t>();
            pk_columns.emplace_back(primary_key.value());
        }
        if(pk_columns.empty()) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }

        // column metadata
        std::map<std::size_t, std::unique_ptr<yugawara::storage::column>> columns_map;  // key: Column.ordinalPosition
        std::map<std::size_t, bool> is_descendant;                                      // key: Column.ordinalPosition
        std::vector<std::size_t> value_columns;                                         // index: received order, content: ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, table.get_child(manager::metadata::Tables::COLUMNS_NODE)) {
            const boost::property_tree::ptree& column = node.second;
            
            auto nullable = column.get_optional<bool>(manager::metadata::Tables::Column::NULLABLE);
            auto data_type_id = column.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::Column::DATA_TYPE_ID);
            auto name = column.get_optional<std::string>(manager::metadata::Tables::Column::NAME);
            auto ordinal_position = column.get_optional<uint64_t>(manager::metadata::Tables::Column::ORDINAL_POSITION);
            if (!nullable || !data_type_id || !name || !ordinal_position) {
                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            auto nullable_value = nullable.value();
            auto data_type_id_value = static_cast<manager::metadata::DataTypes::DataTypesId>(data_type_id.value());
            auto name_value = name.value();
            auto ordinal_position_value = ordinal_position.value();

            if (std::vector<std::size_t>::iterator itr = std::find(pk_columns.begin(), pk_columns.end(), ordinal_position_value); itr != pk_columns.end()) {  // is this pk_column ?
                bool d{false};
                auto direction = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DIRECTION);
                if (direction) {
                    d = static_cast<manager::metadata::Tables::Column::Direction>(direction.value()) == manager::metadata::Tables::Column::Direction::DESCENDANT;
                }
                is_descendant[ordinal_position_value] = d;  // is_descendant will be used in PK section
                if(nullable_value) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // pk_column must not be nullable
                    return;
                }
            } else {  // this is value column
                value_columns.emplace_back(ordinal_position_value);
            }

            switch(data_type_id_value) {  // build yugawara::storage::column
            case manager::metadata::DataTypes::DataTypesId::INT32:
                columns_map[ordinal_position_value] = std::make_unique<yugawara::storage::column>(name_value, takatori::type::int4(), yugawara::variable::nullity(nullable_value)); break;
            case manager::metadata::DataTypes::DataTypesId::INT64:
                columns_map[ordinal_position_value] = std::make_unique<yugawara::storage::column>(name_value, takatori::type::int8(), yugawara::variable::nullity(nullable_value)); break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
                columns_map[ordinal_position_value] = std::make_unique<yugawara::storage::column>(name_value, takatori::type::float4(), yugawara::variable::nullity(nullable_value)); break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                columns_map[ordinal_position_value] = std::make_unique<yugawara::storage::column>(name_value, takatori::type::float8(), yugawara::variable::nullity(nullable_value)); break;
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
            {
                std::size_t data_length_value{1};  // for CHAR
                auto varying = column.get_optional<bool>(manager::metadata::Tables::Column::VARYING);
                if(!varying) {  // varying field is necessary for CHAR/VARCHRA
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    return;
                }
                auto varying_value = varying.value();
                if((!varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::CHAR)) ||
                   (varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::VARCHAR))) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    return;
                }
                auto data_length = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DATA_LENGTH);
                if (!data_length) {
                    if(varying_value) {  // data_length field is necessary for VARCHAR
                        channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                        return;
                    }
                } else {
                    data_length_value = data_length.value();
                }
                columns_map[ordinal_position_value] = std::make_unique<yugawara::storage::column>(name_value, takatori::type::character(takatori::type::varying_t(varying_value), data_length_value), yugawara::variable::nullity(nullable_value)); break;
            }
            default:
                std::abort();  // FIXME
            }
        }

        // build key metadata (yugawara::storage::index::key)
        std::vector<yugawara::storage::index::key> keys;  // index: received order
        for (std::size_t index : pk_columns) {
            auto sort_direction = is_descendant[index] ? takatori::relation::sort_direction::descendant : takatori::relation::sort_direction::ascendant;
            keys.emplace_back(yugawara::storage::index::key(*columns_map[index], sort_direction));
        }

        // build value metadata (yugawara::storage::index::column_ref)
        std::vector<yugawara::storage::index::column_ref> values;
        for(auto &&e : value_columns) {
            values.emplace_back(yugawara::storage::index::column_ref(*columns_map[e]));
        }

#if 0
        std::vector<TableInfo::Column> shakujo_columns;
        for (const auto& [key, value] : columns_map) {
            shakujo_columns.emplace_back(*value);
        }
        std::vector<IndexInfo::Column> shakujo_pk_columns;
        for (const auto& [key, value] : pk_columns) {
            shakujo_pk_columns.emplace_back(*value);
        }

        TableInfo table { table_name.value(), shakujo_columns, shakujo_pk_columns };
        
        auto provider = db_.provider();
        try {
            provider->add(table, false);
        } catch (std::exception &ex) {
            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }
#endif
        channel_->send_ack(ERROR_CODE::OK);
    } else {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

}  // ogawayama::server
