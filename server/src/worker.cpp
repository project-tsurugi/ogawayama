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

#include "worker.h"

namespace ogawayama::server {

DECLARE_string(dbname);

Worker::Worker(umikongo::Database *db, std::size_t id) : db_(db), id_(id)
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
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    try {
        context_->execute_statement(sql);
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

void Worker::send_metadata(std::size_t rid)
{
    using namespace shakujo::common::core;

    auto metadata = cursors_.at(rid).executable_->metadata();
    for (auto& t: metadata->column_types()) {
        switch(t->kind()) {
        case Type::Kind::INT:
            switch((static_cast<type::Numeric const *>(t))->size()) {
            case 16:
                cursors_.at(rid).row_queue_->push_type(TYPE::INT16, 2);
                break;
            case 32:
                cursors_.at(rid).row_queue_->push_type(TYPE::INT32, 4);
                break;
            case 64:
                cursors_.at(rid).row_queue_->push_type(TYPE::INT64, 8);
                break;
            }
            break;
        case Type::Kind::FLOAT:
            switch((static_cast<type::Float const *>(t))->size()) {
            case 32:
                cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT32, 4);
                break;
            case 64:
                cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT64, 8);
                break;
            }
            break;
        case Type::Kind::CHAR:
            cursors_.at(rid).row_queue_->push_type(TYPE::TEXT, (static_cast<type::Char const *>(t))->size());
            break;
        case Type::Kind::STRING:
            cursors_.at(rid).row_queue_->push_type(TYPE::TEXT, INT32_MAX);
            break;
        default:
            std::cerr << "unsurpported data type: " << t->kind() << std::endl;
            break;
        }
    }
}

bool Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    
    try {
        cursor.row_queue_ = std::make_unique<ogawayama::common::RowQueue>
            (shm4_row_queue_->shm_name(ogawayama::common::param::resultset, rid), shm4_row_queue_.get());
        cursor.row_queue_->clear();
    } catch (umikongo::Exception& e) {
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    try {
        cursor.executable_ = db_->compile(sql);
        send_metadata(rid);
        cursor.iterator_ = context_->execute_query(cursor.executable_.get());
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return false;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    return true;
}

void Worker::next(std::size_t rid)
{
    try {
        auto& cursor = cursors_.at(rid);
        auto& rq = cursor.row_queue_;
        if(!cursor.iterator_) { return; }
        std::size_t limit = rq->get_requested();
        for(std::size_t i = 0; i < limit; i++) {
            auto row = cursor.iterator_->next();
            if (row != nullptr) {
                rq->resize_writing_row(rq->get_metadata_ptr()->get_types().size());
                for (auto& t: rq->get_metadata_ptr()->get_types()) {
                    std::size_t cindex = rq->get_cindex();
                    if (row->is_null(cindex)) {
                        rq->put_next_column(std::monostate());
                    } else {
                        switch (t.get_type()) {
                        case TYPE::INT16:
                            rq->put_next_column(row->get<std::int16_t>(cindex)); break;
                        case TYPE::INT32:
                            rq->put_next_column(row->get<std::int32_t>(cindex)); break;
                        case TYPE::INT64:
                            rq->put_next_column(row->get<std::int64_t>(cindex)); break;
                        case TYPE::FLOAT32:
                            rq->put_next_column(row->get<float>(cindex)); break;
                        case TYPE::FLOAT64:
                            rq->put_next_column(row->get<double>(cindex)); break;
                        case TYPE::TEXT:
                            rq->put_next_column(row->get<std::string_view>(cindex)); break;
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
    } catch (umikongo::Exception& e) {
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

void Worker::prepare(std::string_view sql, std::size_t sid)
{
    if (prepared_statements_.size() < (sid + 1)) {
        prepared_statements_.resize(sid + 1);
    }
    try {
        prepared_statements_.at(sid) = db_->prepare(sql);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
    channel_->send_ack(ERROR_CODE::OK);
}

void Worker::set_params(umikongo::PreparedStatement::Parameters *p)
{
    auto& params = parameters_->get_params();
    for(auto& param: params) {
        std::size_t idx = &param - &params[0];
        std::visit([&p, &idx](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                std::string prefix = "p";
                if constexpr (std::is_same_v<T, std::int16_t>) {
                        p->setInt(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, std::int32_t>) {
                        p->setInt32(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, std::int64_t>) {
                        p->setInt64(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, float>) {
                        p->setFloat(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, double>) {
                        p->setDouble(prefix + std::to_string(idx+1), arg);
                    }
                else if constexpr (std::is_same_v<T, ogawayama::common::ShmString>) {
                        p->setString(prefix + std::to_string(idx+1), std::string_view(arg));
                    }
                else if constexpr (std::is_same_v<T, std::monostate>) {
                        p->setNull(prefix + std::to_string(idx+1));
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
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    try {
        auto params = umikongo::PreparedStatement::Parameters::get();
        set_params(params.get());
        context_->execute_statement(prepared_statements_.at(sid).get(), std::move(params));
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

bool Worker::execute_prepared_query(std::size_t sid, std::size_t rid)
{
    if (sid >= prepared_statements_.size()) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }

    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);

    try {
        cursor.row_queue_ = std::make_unique<ogawayama::common::RowQueue>
            (shm4_row_queue_->shm_name(ogawayama::common::param::resultset, rid), shm4_row_queue_.get());
        cursor.row_queue_->clear();
    } catch (umikongo::Exception& e) {
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    try {
        auto params = umikongo::PreparedStatement::Parameters::get();
        set_params(params.get());
        cursor.executable_ = db_->compile(prepared_statements_.at(sid).get(), std::move(params));
        send_metadata(rid);
        cursor.iterator_ = context_->execute_query(cursor.executable_.get());
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return false;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return false;
    }
    return true;
}

void Worker::deploy_metadata(std::size_t table_id)
{
    using namespace shakujo::common::schema;
    using namespace shakujo::common::core;

    manager::metadata::ErrorCode error;

    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(FLAGS_dbname);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(FLAGS_dbname);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
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
        std::map<std::size_t, std::unique_ptr<IndexInfo::Column>> pk_columns;
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, primary_keys) {
            const boost::property_tree::ptree& value = node.second;
            boost::optional<uint64_t> primary_key = value.get_value_optional<uint64_t>();
            pk_columns[primary_key.value()] = nullptr;
        }

        // column metadata
        std::map<std::size_t, std::unique_ptr<TableInfo::Column>> columns_map;
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

            auto itr = pk_columns.find(ordinal_position.value());
            if (itr != pk_columns.end()) {
                auto direction = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DIRECTION);
                Direction shakujo_direction;
                if (direction) {  // 0: DEFAULT, 1: ASCENDANT, 2: DESCENDANT (see manager/metadata-manager/docs/table_metadata.md)
                    shakujo_direction = (direction.value() == 2) ? Direction::DESCENDANT : Direction::ASCENDANT;
                } else {
                    shakujo_direction = Direction::ASCENDANT;
                }
                itr->second = std::make_unique<IndexInfo::Column>(name.value(), shakujo_direction);
            }

            auto shakujo_nullable = nullable.value() ? Type::Nullity::NULLABLE : Type::Nullity::NEVER_NULL;
            std::size_t data_length_value{};
            auto data_type_id_value = static_cast<manager::metadata::DataTypes::DataTypesId>(data_type_id.value());
            switch(data_type_id_value) {  // See manager/metadata-manager/src/datatypes.cpp for specific values of data_type_id
            case manager::metadata::DataTypes::DataTypesId::INT32:
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
                data_length_value = 32; break;
            case manager::metadata::DataTypes::DataTypesId::INT64:
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                data_length_value = 64; break;
            }

            switch(data_type_id_value) {
            case manager::metadata::DataTypes::DataTypesId::INT32:
            case manager::metadata::DataTypes::DataTypesId::INT64:
                {
                    auto shakujo_type = std::make_unique<type::Int>(data_length_value, shakujo_nullable);
                    columns_map[ordinal_position.value()] = std::make_unique<TableInfo::Column>(name.value(), std::move(shakujo_type));
                }
                break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                {
                    auto shakujo_type = std::make_unique<type::Float>(data_length_value, shakujo_nullable);
                    columns_map[ordinal_position.value()] = std::make_unique<TableInfo::Column>(name.value(), std::move(shakujo_type));
                }
                break;
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
                {
                    auto varying = column.get_optional<bool>(manager::metadata::Tables::Column::VARYING);
                    if(!varying) {
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
                        if(varying_value) {
                            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                            return;
                        }
                        data_length_value = 1;  // CHAR
                    } else {
                        data_length_value = data_length.value();
                    }
                    
                    auto shakujo_type = std::make_unique<type::Char>(varying_value, data_length_value, shakujo_nullable);
                    columns_map[ordinal_position.value()] = std::make_unique<TableInfo::Column>(name.value(), std::move(shakujo_type));
                }
                break;
            default:
                {
                    channel_->send_ack(ERROR_CODE::UNKNOWN);
                    return;
                }
            }
        }

        std::vector<TableInfo::Column> shakujo_columns;
        for (const auto& [key, value] : columns_map) {
            shakujo_columns.emplace_back(*value);
        }
        std::vector<IndexInfo::Column> shakujo_pk_columns;
        for (const auto& [key, value] : pk_columns) {
            shakujo_pk_columns.emplace_back(*value);
        }

        TableInfo table { table_name.value(), shakujo_columns, shakujo_pk_columns };
        
        auto provider = db_->provider();
        provider->add(table);

        channel_->send_ack(ERROR_CODE::OK);
    } else {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

}  // ogawayama::server
