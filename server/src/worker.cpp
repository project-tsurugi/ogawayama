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

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "shakujo/common/core/type/Int.h"
#include "shakujo/common/core/type/Float.h"
#include "shakujo/common/core/type/Char.h"
#include "shakujo/common/core/type/String.h"

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

    datatypes_ = std::make_unique<manager::metadata_manager::DataTypeMetadata>(FLAGS_dbname);
    if (datatypes_->load() != manager::metadata_manager::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::SERVER_FAILURE);
        return;
    }
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
        case ogawayama::common::CommandMessage::Type::EXECUTE_CREATE_TABLE:
            execute_create_table(string);
            break;
        case ogawayama::common::CommandMessage::Type::PROVIDE_TABLE_SCHEMA:
            provide_table_schema(string);
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


void Worker::execute_create_table(std::string_view sql)
{
    using namespace manager::metadata_manager;

    execute_statement(sql);

    // get table schema from umikongo
    std::vector<shakujo::common::schema::TableInfo const*> vec{};
    db_->each_table([&](auto& info){
        vec.emplace_back(&info);
    });

    // root
    boost::property_tree::ptree root;
    
    for (auto &t : vec) {
        // table
        boost::property_tree::ptree new_table;

        // name
        auto name = t->name().segments();
        new_table.put(TableMetadata::NAME, name.front().c_str());
        
        std::map<uint64_t, shakujo::common::core::Direction> directions;
        // primaryKey
        boost::property_tree::ptree primary_keys;
        {
            auto primary_index = t->primary_index();
            for (auto &cp : primary_index.columns()) {
                boost::property_tree::ptree primary_key;
                uint64_t ordinal_position = 0, i = 1;
                for (auto &cn : t->columns()) {
                    if (cn.name() == cp.name()) { ordinal_position = i; break; }
                    i++;
                }
                if (ordinal_position == 0) {
                    std::abort();
                }
                primary_key.put<uint64_t>("", ordinal_position);
                primary_keys.push_back(std::make_pair("", primary_key));
                directions[ordinal_position] = cp.direction();
            }
            new_table.add_child(TableMetadata::PRIMARY_KEY_NODE, primary_keys);
        }

        // columns
        boost::property_tree::ptree columns;
        uint64_t ordinal_position = 1;
        for (auto &c : t->columns()) {
            boost::property_tree::ptree column;
            // name
            column.put(TableMetadata::Column::NAME, c.name().c_str());

            // ordinalPosition
            column.put<uint64_t>(TableMetadata::Column::ORDINAL_POSITION, ordinal_position);

            // get dataTypeId
            ErrorCode err;
            boost::property_tree::ptree datatype;
            auto t = c.type();
            switch(t->kind()) {
            case shakujo::common::core::Type::Kind::INT:
                switch((static_cast<shakujo::common::core::type::Numeric const *>(t))->size()) {
                case 16:
                    err = datatypes_->get("INT16", datatype); break;
                case 32:
                    err = datatypes_->get("INT32", datatype); break;
                case 64:
                    err = datatypes_->get("INT64", datatype); break;
                }
                break;
            case shakujo::common::core::Type::Kind::FLOAT:
                switch((static_cast<shakujo::common::core::type::Float const *>(t))->size()) {
                case 32:
                    err = datatypes_->get("FLOAT32", datatype); break;
                case 64:
                    err = datatypes_->get("FLOAT64", datatype); break;
                }
                break;
            case shakujo::common::core::Type::Kind::CHAR:
                {
                    err = datatypes_->get("TEXT", datatype);
                    auto ct = static_cast<shakujo::common::core::type::Char const *>(t);
                    // dataLength
                    column.put<uint64_t>(TableMetadata::Column::DATA_LENGTH, ct->size());
                    // varying
                    column.put<bool>(TableMetadata::Column::VARYING, ct->varying());
                }
                break;
            default:
                std::abort();
            }
            if (err != ErrorCode::OK) {
                std::abort();
            }
            ObjectIdType data_type_id = datatype.get<ObjectIdType>(DataTypeMetadata::ID);
            if (!data_type_id) {
                std::abort();
            }
            // put dataTypeId
            column.put<ObjectIdType>(TableMetadata::Column::DATA_TYPE_ID, data_type_id);

            // nullable
            column.put<bool>(TableMetadata::Column::NULLABLE, t->nullable());
            
            // direction
            auto it = directions.find(ordinal_position);
            if (it != directions.end()) {
                column.put<uint64_t>(TableMetadata::Column::DIRECTION,
                                     it->second == shakujo::common::core::Direction::ASCENDANT ? 1 : 2);
            } else {
                column.put<uint64_t>(TableMetadata::Column::DIRECTION, 0);
            }

            // default
            column.put(TableMetadata::Column::DEFAULT, to_string(c.default_value()).c_str());
                
            columns.push_back(std::make_pair("", column));
            ordinal_position++;
        }
        new_table.add_child(TableMetadata::COLUMNS_NODE, columns);

        root.add_child(TableMetadata::TABLES_NODE, new_table);
    }

    if (TableMetadata::save(FLAGS_dbname, root) != ErrorCode::OK) {
        std::abort();
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

using namespace shakujo::common::schema;
using namespace shakujo::common::core;

void Worker::provide_table_schema([[maybe_unused]]std::string_view sql)
{
    TableInfo::Column columnCI16 {"CI16", std::make_unique<type::Int>(16U, Type::Nullity::NEVER_NULL)};
    TableInfo::Column columnCI32 {"CI32", std::make_unique<type::Int>(32U, Type::Nullity::NEVER_NULL)};
    TableInfo::Column columnCI64 {"CI64", std::make_unique<type::Int>(64U, Type::Nullity::NEVER_NULL)};

    TableInfo::Column columnCF32 {"CF32", std::make_unique<type::Float>(32U, Type::Nullity::NEVER_NULL)};
    TableInfo::Column columnCF64 {"CF64", std::make_unique<type::Float>(64U, Type::Nullity::NEVER_NULL)};

    TableInfo::Column columnCS {"CS", std::make_unique<type::String>(Type::Nullity::NEVER_NULL)};
    TableInfo::Column columnCC {"CC", std::make_unique<type::Char>(false, 24, Type::Nullity::NEVER_NULL)};
    TableInfo::Column columnCV {"CV", std::make_unique<type::Char>(true, 40, Type::Nullity::NEVER_NULL)};

    std::vector<TableInfo::Column> columns;
    columns.emplace_back(columnCI16);
    columns.emplace_back(columnCI32);
    columns.emplace_back(columnCI64);
    columns.emplace_back(columnCF32);
    columns.emplace_back(columnCF64);
    columns.emplace_back(columnCS);
    columns.emplace_back(columnCC);
    columns.emplace_back(columnCV);

    TableInfo table { "T5", columns };

    auto provider = db_->provider();
    provider->add(table);
    provider->remove(Name("T5"));


    // TPC-C WAREHOUSE table
    provider->add(TableInfo {
            "WAREHOUSE", std::vector<TableInfo::Column> {
                TableInfo::Column{"w_id", std::make_unique<type::Int>(32U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_name", std::make_unique<type::Char>(false, 10U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_street_1", std::make_unique<type::String>(Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_street_2", std::make_unique<type::String>(Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_city", std::make_unique<type::Char>(true, 20U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_state", std::make_unique<type::Char>(false, 2U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_zip", std::make_unique<type::Char>(false, 9U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_tax", std::make_unique<type::Float>(64U, Type::Nullity::NEVER_NULL)},
                TableInfo::Column{"w_ytd", std::make_unique<type::Float>(64U)}
            },
                { //primary key
                    {
                        {"w_id"}
                    }
                }
        });

    channel_->send_ack(ERROR_CODE::OK);
}

}  // ogawayama::server
