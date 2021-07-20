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

#include <vector>
#include <boost/foreach.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <msgpack.hpp>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>

#include "worker.h"

namespace ogawayama::server {

DECLARE_string(dbname);

void Worker::run()
{
    request::Request request;

    while(true) {

        auto h = request_wire_container_.peep(true);
        std::size_t length = h.get_length();
        std::string msg;
        msg.resize(length);
        request_wire_container_.read(reinterpret_cast<signed char*>(msg.data()), length);
        if (!request.ParseFromString(msg)) { LOG(ERROR) << "parse error" << std::endl; }
        else { VLOG(1) << "s:" << request.session_handle().handle() << "-" << "idx:" << h.get_idx() << std::endl; }

        switch (request.request_case()) {
        case request::Request::RequestCase::kBegin:
            VLOG(1) << "begin" << std::endl;
            {
                if (!transaction_) {
                    if (transaction_ = db_.create_transaction(); transaction_ != nullptr) {
                        ::common::Transaction t;
                        protocol::Begin b;
                        protocol::Response r;

                        t.set_handle(++transaction_id_);
                        b.set_allocated_transaction_handle(&t);
                        r.set_allocated_begin(&b);
                        reply(r, h.get_idx());
                        r.release_begin();
                        b.release_transaction_handle();
                    } else {
                        error<protocol::Begin>("error in db_.create_transaction()", h.get_idx());
                    }
                } else {
                    error<protocol::Begin>("transaction has already begun", h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kPrepare:
            VLOG(1) << "prepare" << std::endl;
            {
                std::size_t sid = prepared_statements_index_;

                auto pp = request.mutable_prepare();
                auto hvs = pp->mutable_host_variables();
                auto sql = pp->mutable_sql();
                VLOG(1) << "idx:" << h.get_idx() << " : "
                        << *sql
                        << std::endl;
                if (prepared_statements_.size() < (sid + 1)) {
                    prepared_statements_.resize(sid + 1);
                }

                for(std::size_t i = 0; i < hvs->variables_size() ;i++) {
                    auto hv = hvs->mutable_variables(i);
                    switch(hv->type()) {
                    case ::common::DataType::INT4:
                        db_.register_variable(hv->name(), jogasaki::api::field_type_kind::int4);
                        break;
                    case ::common::DataType::INT8:
                        db_.register_variable(hv->name(), jogasaki::api::field_type_kind::int8);
                        break;
                    case ::common::DataType::FLOAT4:
                        db_.register_variable(hv->name(), jogasaki::api::field_type_kind::float4);
                        break;
                    case ::common::DataType::FLOAT8:
                        db_.register_variable(hv->name(), jogasaki::api::field_type_kind::float8);
                        break;
                    case ::common::DataType::STRING:
                        db_.register_variable(hv->name(), jogasaki::api::field_type_kind::character);
                        break;
                    }
                }
                if(auto rc = db_.prepare(*sql, prepared_statements_.at(sid)); rc == jogasaki::status::ok) {
                    ::common::PreparedStatement ps;
                    protocol::Prepare p;
                    protocol::Response r;

                    ps.set_handle(sid);
                    *p.mutable_prepared_statement_handle() = ps;
                    r.set_allocated_prepare(&p);
                    reply(r, h.get_idx());
                    r.release_prepare();
                    p.release_prepared_statement_handle();

                    prepared_statements_index_ = sid + 1;
                } else {
                    error<protocol::Prepare>("error in db_.prepare()" , h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kExecuteStatement:
            VLOG(1) << "execute_statement" << std::endl;
            {
                auto eq = request.mutable_execute_statement();
                VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                        << "idx:" << h.get_idx() << " : "
                        << *(eq->mutable_sql())
                        << std::endl;
                if (auto err = execute_statement(*(eq->mutable_sql())); err == nullptr) {
                    protocol::Success s;
                    protocol::ResultOnly ok;
                    protocol::Response r;

                    ok.set_allocated_success(&s);
                    r.set_allocated_result_only(&ok);
                    reply(r, h.get_idx());
                    r.release_result_only();
                    ok.release_success();
                } else {
                    error<protocol::ResultOnly>(err, h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kExecuteQuery:
            VLOG(1) << "execute_query" << std::endl;
            {
                auto eq = request.mutable_execute_query();
                VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                        << "idx:" << h.get_idx() << " : "
                        << *(eq->mutable_sql())
                        << std::endl;

                if (auto err = execute_query(*(eq->mutable_sql()), ++resultset_id_); err == nullptr) {
                    protocol::ExecuteQuery e;
                    protocol::Response r;

                    e.set_name(cursors_.at(resultset_id_).wire_name_);
                    r.set_allocated_execute_query(&e);
                    reply(r, h.get_idx());
                    r.release_execute_query();

                    next(resultset_id_);
                } else {
                    error<protocol::ExecuteQuery>(err, h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kExecutePreparedStatement:
            VLOG(1) << "execute_prepared_statement" << std::endl;
            {
                auto pq = request.mutable_execute_prepared_statement();
                auto ph = pq->mutable_prepared_statement_handle();
                auto sid = ph->handle();
                VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle() << "-"
                        << "idx:" << h.get_idx() << " : "
                        << "sid:" << sid
                        << std::endl;

                auto params = jogasaki::api::create_parameter_set();
                set_params(pq->mutable_parameters(), params);
                if (auto err = execute_prepared_statement(sid, *params); err == nullptr) {
                    protocol::Success s;
                    protocol::ResultOnly ok;
                    protocol::Response r;

                    ok.set_allocated_success(&s);
                    r.set_allocated_result_only(&ok);
                    reply(r, h.get_idx());
                    r.release_result_only();
                    ok.release_success();
                } else {
                    error<protocol::ResultOnly>(err, h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kExecutePreparedQuery:
            VLOG(1) << "execute_prepared_query" << std::endl;
            {
                auto pq = request.mutable_execute_prepared_query();
                auto ph = pq->mutable_prepared_statement_handle();
                auto sid = ph->handle();
                VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle() << "-"
                        << "idx:" << h.get_idx() << " : "
                        << "sid:" << sid
                        << std::endl;

                auto params = jogasaki::api::create_parameter_set();
                set_params(pq->mutable_parameters(), params);

                if(auto err = execute_prepared_query(sid, *params, ++resultset_id_); err == nullptr) {
                    protocol::ExecuteQuery e;
                    protocol::Response r;

                    e.set_name(cursors_.at(resultset_id_).wire_name_);
                    r.set_allocated_execute_query(&e);
                    reply(r, h.get_idx());
                    r.release_execute_query();

                    next(resultset_id_);
                } else {
                    error<protocol::ExecuteQuery>(err, h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kCommit:
            VLOG(1) << "commit" << std::endl;
            {
                if (transaction_) {
                    if(auto rc = transaction_->commit(); rc == jogasaki::status::ok) {
                        auto eq = request.mutable_commit();
                        VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                                << "idx:" << h.get_idx()
                                << std::endl;

                        protocol::Success s;
                        protocol::ResultOnly ok;
                        protocol::Response r;

                        ok.set_allocated_success(&s);
                        r.set_allocated_result_only(&ok);
                        reply(r, h.get_idx());
                        r.release_result_only();
                        ok.release_success();

                        transaction_ = nullptr;
                    } else {
                        error<protocol::ResultOnly>("error in transaction_->commit()", h.get_idx());
                    }
                } else {
                    error<protocol::ResultOnly>("transaction has not begun", h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kRollback:
            VLOG(1) << "rollback" << std::endl;
            break;
        case request::Request::RequestCase::kDisposePreparedStatement:
            VLOG(1) << "dispose_prepared_statement" << std::endl;
            {
                auto dp = request.mutable_dispose_prepared_statement();
                auto ph = dp->mutable_prepared_statement_handle();
                auto sid = ph->handle();

                VLOG(1) << "idx:" << h.get_idx() << " : "
                        << "ps:" << sid
                        << std::endl;

                if(prepared_statements_.size() > sid) {
                    if(prepared_statements_.at(sid)) {
                        prepared_statements_.at(sid) = nullptr;

                        protocol::Success s;
                        protocol::ResultOnly ok;
                        protocol::Response r;

                        ok.set_allocated_success(&s);
                        r.set_allocated_result_only(&ok);
                        reply(r, h.get_idx());
                        r.release_result_only();
                        ok.release_success();
                    } else {
                        error<protocol::ResultOnly>("cannot find prepared statement with the index given", h.get_idx());
                    }
                } else {
                    error<protocol::ResultOnly>("index is larger than the number of prepred statment registerd", h.get_idx());
                }
            }
            break;
        case request::Request::RequestCase::kDisconnect:
            VLOG(1) << "disconnect" << std::endl;
            {
                protocol::Success s;
                protocol::ResultOnly ok;
                protocol::Response r;

                ok.set_allocated_success(&s);
                r.set_allocated_result_only(&ok);
                reply(r, h.get_idx());
                r.release_result_only();
                ok.release_success();
            }
            return;
        case request::Request::RequestCase::REQUEST_NOT_SET:
            VLOG(1) << "not used" << std::endl;
            break;
        default:
            LOG(ERROR) << "????" << std::endl;
            break;
        }
    }
}

const char* Worker::execute_statement(std::string_view sql)
{
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.create_executable(sql, e); rc != jogasaki::status::ok) {
        return "error in db_.create_executable()";
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

void Worker::send_metadata(std::size_t rid)
{
    auto metadata = cursors_.at(rid).result_set_->meta();
    std::size_t n = metadata->field_count();
    schema::RecordMeta meta;

    for (std::size_t i = 0; i < n; i++) {
        auto* column = new schema::RecordMeta_Column;
        switch(metadata->at(i).kind()) {
        case jogasaki::api::field_type_kind::int4:
            column->set_type(::common::DataType::INT4);
            column->set_nullable(metadata->nullable(i));
            *meta.add_columns() = *column;
        break;
        case jogasaki::api::field_type_kind::int8:
            column->set_type(::common::DataType::INT8);
            column->set_nullable(metadata->nullable(i));
            *meta.add_columns() = *column;
        break;
        case jogasaki::api::field_type_kind::float4:
            column->set_type(::common::DataType::FLOAT4);
            column->set_nullable(metadata->nullable(i));
            *meta.add_columns() = *column;
        break;
        case jogasaki::api::field_type_kind::float8:
            column->set_type(::common::DataType::FLOAT8);
            column->set_nullable(metadata->nullable(i));
            *meta.add_columns() = *column;
        break;
        case jogasaki::api::field_type_kind::character:
            column->set_type(::common::DataType::STRING);
            column->set_nullable(metadata->nullable(i));
            *meta.add_columns() = *column;
        break;
        default:
        std::cout << __LINE__ << ":" << i << std::endl;
            std::cerr << "unsurpported data type: " << metadata->at(i).kind() << std::endl;
            break;
        }
    }

    std::string output;
    if (!meta.SerializeToString(&output)) { std::abort(); }
    cursors_.at(rid).resultset_wire_container_->write(reinterpret_cast<const signed char*>(output.data()),
                                            tsubakuro::common::wire::length_header(output.size()));
}

const char* Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_.create_transaction();
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    cursor.wire_name_ = std::string("resultset-");
    cursor.wire_name_ += std::to_string(rid);
    cursor.resultset_wire_container_ = wire_->create_resultset_wire(cursor.wire_name_);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.create_executable(sql, e); rc != jogasaki::status::ok) {
        return "error in db_.create_executable()";
    }
    auto& rs = cursor.result_set_;
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    send_metadata(rid);

    return nullptr;
}

void Worker::next(std::size_t rid)
{
    auto& cursor = cursors_.at(rid);
    tsubakuro::common::wire::server_wire_container::resultset_wire_container& wire = *cursor.resultset_wire_container_;
    const jogasaki::api::record_meta* meta = cursor.result_set_->meta();
    auto iterator = cursor.result_set_->iterator();
    while(true) {
        auto record = iterator->next();
        if (record != nullptr) {
            for (std::size_t cindex = 0; cindex < meta->field_count(); cindex++) {
                if (record->is_null(cindex)) {
                    msgpack::pack(wire, msgpack::type::nil_t());
                } else {
                    switch (meta->at(cindex).kind()) {
                    case jogasaki::api::field_type_kind::int4:
                        msgpack::pack(wire, record->get_int4(cindex)); break;
                    case jogasaki::api::field_type_kind::int8:
                        msgpack::pack(wire, record->get_int8(cindex)); break;
                    case jogasaki::api::field_type_kind::float4:
                        msgpack::pack(wire, record->get_float4(cindex)); break;
                    case jogasaki::api::field_type_kind::float8:
                        msgpack::pack(wire, record->get_float8(cindex)); break;
                    case jogasaki::api::field_type_kind::character:
                        msgpack::pack(wire, record->get_character(cindex)); break;
                    default:
                        std::cerr << "type undefined" << std::endl; break;
                    }
                }
            }
        } else {
            VLOG(1) << "detect eor" << std::endl;
            wire.set_eor();
            break;
        }
    }
}

void Worker::set_params(request::ParameterSet* ps, std::unique_ptr<jogasaki::api::parameter_set>& params)
{
    for (std::size_t i = 0; i < ps->parameters_size() ;i++) {
        auto p = ps->mutable_parameters(i);
        switch (p->value_case()) {
        case request::ParameterSet::Parameter::ValueCase::kIValue:
            params->set_int4(p->name(), p->l_value());
            break;
        case request::ParameterSet::Parameter::ValueCase::kLValue:
            params->set_int8(p->name(), p->l_value());
            break;
        case request::ParameterSet::Parameter::ValueCase::kFValue:
            params->set_float4(p->name(), p->l_value());
            break;
        case request::ParameterSet::Parameter::ValueCase::kDValue:
            params->set_float8(p->name(), p->l_value());
            break;
        case request::ParameterSet::Parameter::ValueCase::kSValue:
            params->set_character(p->name(), p->s_value());
            break;
        default:
            std::cerr << "type undefined" << std::endl;
            std::abort();
            break;
        }
    }
}

const char* Worker::execute_prepared_statement(std::size_t sid, jogasaki::api::parameter_set& params)
{
    if (!transaction_) {
        transaction_ = db_.create_transaction();
    }

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.resolve(*prepared_statements_.at(sid), params, e); rc != jogasaki::status::ok) {
        return "error in db_.resolve()";
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

const char* Worker::execute_prepared_query(std::size_t sid, jogasaki::api::parameter_set& params, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_.create_transaction();
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    cursor.wire_name_ = std::string("resultset-");
    cursor.wire_name_ += std::to_string(rid);
    cursor.resultset_wire_container_ = wire_->create_resultset_wire(cursor.wire_name_);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_.resolve(*prepared_statements_.at(sid), params, e); rc != jogasaki::status::ok) {
        return "error in db_.resolve()";
    }

    auto& rs = cursor.result_set_;
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    send_metadata(rid);
    return nullptr;
}

void Worker::deploy_metadata(std::size_t table_id)
{
    manager::metadata::ErrorCode error;

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(FLAGS_dbname);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
//        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(FLAGS_dbname);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
//        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }

    boost::property_tree::ptree table;
    if ((error = tables->get(table_id, table)) == manager::metadata::ErrorCode::OK) {

        // table metadata
        auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
        auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!id || !table_name || (id.value() != table_id)) {
//            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
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
//            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }

        // column metadata
        std::map<std::size_t, const boost::property_tree::ptree*> columns_map;          // key: Column.ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, table.get_child(manager::metadata::Tables::COLUMNS_NODE)) {
            auto ordinal_position = node.second.get_optional<uint64_t>(manager::metadata::Tables::Column::ORDINAL_POSITION);
            if (!ordinal_position) {
//                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            columns_map[ordinal_position.value()] = &node.second;
        }

        takatori::util::reference_vector<yugawara::storage::column> columns;            // index: ordinalPosition order (the first value of index and order are different)
        std::vector<std::size_t> value_columns;                                         // index: received order, content: ordinalPosition of the column
        std::vector<bool> is_descendant;                                                // index: ordinalPosition order (the first value of index and order are different)
        std::size_t ordinal_position_value = 1;
        for(auto &&e : columns_map) {
            if (ordinal_position_value != e.first) {
//                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            const boost::property_tree::ptree& column = *e.second;

            auto nullable = column.get_optional<bool>(manager::metadata::Tables::Column::NULLABLE);
            auto data_type_id = column.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::Column::DATA_TYPE_ID);
            auto name = column.get_optional<std::string>(manager::metadata::Tables::Column::NAME);
            auto ordinal_position = column.get_optional<uint64_t>(manager::metadata::Tables::Column::ORDINAL_POSITION);
            if (!nullable || !data_type_id || !name) {
//                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            auto nullable_value = nullable.value();
            auto data_type_id_value = static_cast<manager::metadata::DataTypes::DataTypesId>(data_type_id.value());
            auto name_value = name.value();

            if (std::vector<std::size_t>::iterator itr = std::find(pk_columns.begin(), pk_columns.end(), ordinal_position_value); itr != pk_columns.end()) {  // is this pk_column ?
                if(nullable_value) {
//                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // pk_column must not be nullable
                    return;
                }
            } else {  // this is value column
                value_columns.emplace_back(ordinal_position_value);
            }

            bool d{false};
            auto direction = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DIRECTION);
            if (direction) {
                d = static_cast<manager::metadata::Tables::Column::Direction>(direction.value()) == manager::metadata::Tables::Column::Direction::DESCENDANT;
            }
            is_descendant.emplace_back(d);                              // is_descendant will be used in PK section

            switch(data_type_id_value) {  // build yugawara::storage::column
            case manager::metadata::DataTypes::DataTypesId::INT32:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int4(), yugawara::variable::nullity(nullable_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::INT64:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int8(), yugawara::variable::nullity(nullable_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float4(), yugawara::variable::nullity(nullable_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float8(), yugawara::variable::nullity(nullable_value)));
                break;
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
            {
                std::size_t data_length_value{1};  // for CHAR
                auto varying = column.get_optional<bool>(manager::metadata::Tables::Column::VARYING);
                if(!varying) {  // varying field is necessary for CHAR/VARCHRA
//                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    return;
                }
                auto varying_value = varying.value();
                if((!varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::CHAR)) ||
                   (varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::VARCHAR))) {
//                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                    return;
                }
                auto data_length = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DATA_LENGTH);
                if (!data_length) {
                    if(varying_value) {  // data_length field is necessary for VARCHAR
//                        channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                        return;
                    }
                } else {
                    data_length_value = data_length.value();
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                                    takatori::type::character(takatori::type::varying_t(varying_value), data_length_value),
                                                                    yugawara::variable::nullity(nullable_value)));
                break;
            }
            default:
                std::abort();  // FIXME
            }
            ordinal_position_value++;
        }

        auto t = std::make_shared<yugawara::storage::table>(yugawara::storage::table::simple_name_type(table_name.value()), std::move(columns));
        if (auto rc = db_.create_table(t); rc != jogasaki::status::ok) {
//            channel_->send_ack((rc == jogasaki::status::err_already_exists) ? ERROR_CODE::INVALID_PARAMETER : ERROR_CODE::UNKNOWN);
            return;
        }

        // build key metadata (yugawara::storage::index::key)
        std::vector<yugawara::storage::index::key> keys;
        for (std::size_t position : pk_columns) {
            auto sort_direction = is_descendant[position-1] ? takatori::relation::sort_direction::descendant : takatori::relation::sort_direction::ascendant;
            keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1], sort_direction));
        }

        // build value metadata (yugawara::storage::index::column_ref)
        std::vector<yugawara::storage::index::column_ref> values;
        for(std::size_t position : value_columns) {
            values.emplace_back(yugawara::storage::index::column_ref(t->columns()[position-1]));
        }

        auto i = std::make_shared<yugawara::storage::index>(
            t,
            yugawara::storage::index::simple_name_type(table_name.value()),
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
//            channel_->send_ack(ERROR_CODE::UNKNOWN);
            return;
        }

//        channel_->send_ack(ERROR_CODE::OK);
    } else {
//        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

}  // ogawayama::server
