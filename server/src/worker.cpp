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


manager::metadata::ErrorCode display_table_metadata_object(const boost::property_tree::ptree& table)
{
    using namespace manager::metadata;
    using namespace boost::property_tree;

    ErrorCode error = ErrorCode::OK;

    auto datatypes = std::make_unique<DataTypes>(FLAGS_dbname);
    error = datatypes->Metadata::load();
    if (error != ErrorCode::OK) {
        std::cerr << "error at " << __LINE__ << std::endl;
        return error;
    }

    ptree datatype;

    // table metadata
    std::cout << "--- table ---" << std::endl;
    boost::optional<ObjectIdType> id =
        table.get_optional<ObjectIdType>(Tables::ID);
    if (id) {
        std::cout << "id : " << id.get() << std::endl;
    }

    boost::optional<std::string> name =
        table.get_optional<std::string>(Tables::NAME);
    if (name) {
        std::cout << "name : " << name << std::endl;
    }

    ptree primary_keys = table.get_child(Tables::PRIMARY_KEY_NODE);
    BOOST_FOREACH (const ptree::value_type& node, primary_keys) {
        std::cout << "primary_key : " << node.second.data() << std::endl;
    }

    // column metadata
    std::cout << "--- columns ---" << std::endl;
    BOOST_FOREACH (const ptree::value_type& node, table.get_child(Tables::COLUMNS_NODE)) {
        const ptree& column = node.second;

        boost::optional<ObjectIdType> id =
            column.get_optional<ObjectIdType>(Tables::Column::ID);
        if (id) {
            std::cout << "id : " << id << std::endl;
        }

        boost::optional<ObjectIdType> table_id =
            column.get_optional<ObjectIdType>(Tables::Column::TABLE_ID);
        if (table_id) {
            std::cout << "table id : " << table_id << std::endl;
        }

        boost::optional<std::string> name =
            column.get_optional<std::string>(Tables::Column::NAME);
        if (name) {
            std::cout << "name : " << name << std::endl;
        }

        boost::optional<uint64_t> ordinal_position =
            column.get_optional<uint64_t>(Tables::Column::ORDINAL_POSITION);
        if (ordinal_position) {
            std::cout << "ordinal position : " << ordinal_position << std::endl;
        }

        boost::optional<ObjectIdType> data_type_id =
            column.get_optional<ObjectIdType>(Tables::Column::DATA_TYPE_ID);
        if (data_type_id) {
            std::cout << "datatype id : " << data_type_id << std::endl;
            datatypes->get(data_type_id.get(), datatype);
            std::cout << "datatype name : "
            << datatype.get<std::string>(DataTypes::NAME) << std::endl;
        }

        boost::optional<uint64_t> data_length =
            column.get_optional<uint64_t>(Tables::Column::DATA_LENGTH);
        if (data_length) {
            std::cout << "data length : " << data_length << std::endl;
        }

        boost::optional<bool> varying =
            column.get_optional<bool>(Tables::Column::VARYING);
        if (varying) {
            std::cout << "varying : " << varying << std::endl;
        }

        boost::optional<bool> nullable =
            column.get_optional<bool>(Tables::Column::NULLABLE);
        if (nullable) {
            std::cout << "nullable : " << nullable << std::endl;
        }

        boost::optional<std::string> default_expr =
            column.get_optional<std::string>(Tables::Column::DEFAULT);
        if (default_expr) {
            std::cout << "default : " << default_expr << std::endl;
        }

        boost::optional<uint64_t> direction =
            column.get_optional<uint64_t>(Tables::Column::DIRECTION);
        if (direction) {
            std::cout << "direction : " << direction << std::endl;
        }

        std::cout << "---------------" << std::endl;
    }

    return ErrorCode::OK;;
}

void Worker::deploy_metadata(std::size_t id)
{
    manager::metadata::ErrorCode error = manager::metadata::ErrorCode::UNKNOWN;

    auto tables = std::make_unique<manager::metadata::Tables>(FLAGS_dbname);   // use Template-Method.
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        return;
    }

    std::cout << "--- table-metadata to read. ---" << std::endl;

    boost::property_tree::ptree table;
    if ((error = tables->get(id, table)) == manager::metadata::ErrorCode::OK) {
        display_table_metadata_object(table);
        std::cout << std::endl;
    }

#if 0
    boost::property_tree::ptree root;
    manager::metadata::ErrorCode error = manager::metadata::Tables::load(FLAGS_dbname.c_str() ,root);
    if (error != manager::metadata::ErrorCode::OK) {
        channel_->send_ack(ERROR_CODE::UNKNOWN);
        return;
    }

    boost::property_tree::ptree tables = root.get_child(manager::metadata::Tables::TABLES_NODE);
    BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, tables) {
        const boost::property_tree::ptree& table = node.second;

        boost::optional<std::string> name =
        table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!name) {
            channel_->send_ack(ERROR_CODE::UNKNOWN);
            return;
        }

        boost::property_tree::ptree primary_keys = table.get_child(manager::metadata::Tables::PRIMARY_KEY_NODE);
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, primary_keys) {
            const boost::property_tree::ptree& value = node.second;
            boost::optional<uint64_t> primary_key = value.get_value_optional<uint64_t>();
        }

        boost::property_tree::ptree columns = table.get_child(manager::metadata::Tables::COLUMNS_NODE);
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, columns) {
            const boost::property_tree::ptree& column = node.second;

            boost::optional<std::string> name =
                column.get_optional<std::string>(manager::metadata::Tables::Column::NAME);
            if (!name) {
                channel_->send_ack(ERROR_CODE::UNKNOWN);
                return;
            }
        }
    }

    using namespace shakujo::common::schema;
    using namespace shakujo::common::core;

    TableInfo::Column columnCI16 {"CI16", std::make_unique<type::Int>(16U, Type::Nullity::NEVER_NULL)};
    std::vector<TableInfo::Column> columns;
    columns.emplace_back(columnCI16);

    TableInfo table { "T5", columns };
    auto provider = db_->provider();
    
    provider->add(table);
#endif
}

}  // ogawayama::server
