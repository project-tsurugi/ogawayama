/*
 * Copyright 2019-2023 tsurugi project.
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
#include <sstream>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <ogawayama/logging.h>

#include <takatori/type/int.h>
#include <takatori/value/int.h>
#include <takatori/type/float.h>
#include <takatori/value/float.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/type/decimal.h>
#include <takatori/type/date.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/datetime_interval.h>
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
    channel_->send_ack(ERROR_CODE::OK);
}

void Worker::run()
{
    while(true) {
        ogawayama::common::CommandMessage::Type type;
        std::size_t ivalue{};
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

static void get_data_length_vector(const boost::property_tree::ptree& column, std::vector<boost::optional<int64_t>>& data_length_vector) {
    boost::property_tree::ptree data_length_array = column.get_child(manager::metadata::Column::DATA_LENGTH);
    BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, data_length_array) {
        data_length_vector.emplace_back(node.second.get_value_optional<int64_t>());
    }
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

            // handling default value parameter for the column
            std::string default_expression_value{};
            bool is_funcexpr{};
            auto default_expression = column.get_optional<std::string>(manager::metadata::Column::DEFAULT_EXPR);
            if (default_expression) {
                default_expression_value = default_expression.value();
                auto is_funcexpr_opt = column.get_optional<bool>(manager::metadata::Column::IS_FUNCEXPR);
                if (!is_funcexpr_opt) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // IS_FUNCEXPR is necessary when default_expression is given
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return;
                }
                is_funcexpr = is_funcexpr_opt.value();
                if (is_funcexpr) {
                    LOG(ERROR) << " default value in function form is not currentry surported";
                }
            }

            if (auto itr = std::find(pk_columns.begin(), pk_columns.end(), column_number_value); itr != pk_columns.end()) {  // is this pk_column ?
                if(!is_not_null_value) {
                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // pk_column must be is_not_null
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return;
                }
            } else {  // this is value column
                value_columns.emplace_back(column_number_value);
            }
            VLOG(log_debug) << " found column: name = " << name_value << ", data_type_id = " << static_cast<std::size_t>(data_type_id_value) << ", is_not_null = " << (is_not_null_value ? "not_null" : "nullable");  // NOLINT

            switch(data_type_id_value) {  // build yugawara::storage::column
            case manager::metadata::DataTypes::DataTypesId::INT32:
            {
                // int column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::int32_t value{};
                    std::istringstream(default_expression_value) >> value;
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::int4>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int4(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::INT64:
            {
                // bigint column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::int64_t value{};
                    std::istringstream(default_expression_value) >> value;
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::int8>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int8(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::FLOAT32:
            {
                // real column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    float value{};
                    std::istringstream(default_expression_value) >> value;
                    yugawara::storage::column_value default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::float4>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float4(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
            {
                // double precision column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    double value{};
                    std::istringstream(default_expression_value) >> value;
                    yugawara::storage::column_value default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::float8>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float8(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
            {
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
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";  // type and attributes are inconsistent
                    return;
                }

                std::size_t data_length_value{1};  // for CHAR
                bool use_default_length = false;
                std::vector<boost::optional<int64_t>> data_length_vector;
                get_data_length_vector(column, data_length_vector);
                if (data_length_vector.size() == 0) {
                    if(varying_value) {  // no data_length field, use default
                        use_default_length = true;
                    }
                } else {
                    auto data_length = data_length_vector.at(0);
                    if (!data_length) {
                        if(varying_value) {  // no data_length_value, use default
                            use_default_length = true;
                        }
                    } else {
                        data_length_value = data_length.value();
                    }
                }

                // character column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind((data_type_id_value == manager::metadata::DataTypes::DataTypesId::CHAR) ? "::bpchar" : "::character varying");
                    if (index == std::string::npos) {
                        channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return;
                    }
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::character const>(default_expression_value.substr(1, default_expression_value.length() - index)));
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               (use_default_length ?
                                                                takatori::type::character(takatori::type::varying_t(varying_value)) :
                                                                takatori::type::character(takatori::type::varying_t(varying_value), data_length_value)),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::NUMERIC:  // decimal
            {
                std::size_t p{}, s{};
                std::vector<boost::optional<int64_t>> data_length_vector;
                get_data_length_vector(column, data_length_vector);
                if (data_length_vector.size() == 0) {
                    columns.emplace_back(yugawara::storage::column(name_value,
                                                                   takatori::type::decimal(),
                                                                   yugawara::variable::nullity(!is_not_null_value)));
                } else {
                    auto po = data_length_vector.at(0);
                    if (!po) {
                        channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                        VLOG(log_debug) << "<-- UNSUPPORTED";
                        return;
                    }
                    p = po.value();
                    if (data_length_vector.size() >= 2) {
                        auto so = data_length_vector.at(0);
                        if (!so) {
                            s = so.value();
                        }
                    }
                    columns.emplace_back(yugawara::storage::column(name_value,
                                                                   takatori::type::decimal(p, s),
                                                                   yugawara::variable::nullity(!is_not_null_value)));
                }
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::DATE:  // date
            {
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::date(),
                                                               yugawara::variable::nullity(!is_not_null_value)));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIME:  // time_of_day
            case manager::metadata::DataTypes::DataTypesId::TIMETZ:  // time_of_day
            {
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_of_day(
                                                                   data_type_id_value == manager::metadata::DataTypes::DataTypesId::TIMETZ ?
                                                                   takatori::type::with_time_zone : ~takatori::type::with_time_zone
                                                               ),
                                                               yugawara::variable::nullity(!is_not_null_value)));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIMESTAMP:  // time_point
            case manager::metadata::DataTypes::DataTypesId::TIMESTAMPTZ:  // time_point
            {
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_point(
                                                                   data_type_id_value == manager::metadata::DataTypes::DataTypesId::TIMESTAMPTZ ?
                                                                   takatori::type::with_time_zone : ~takatori::type::with_time_zone
                                                               ),
                                                               yugawara::variable::nullity(!is_not_null_value)));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::INTERVAL:  // datetime_interval
            {
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::datetime_interval(),
                                                               yugawara::variable::nullity(!is_not_null_value)));
                break;
            }
            default:
                std::abort();  // FIXME
            }  // switch(data_type_id_value) {
            column_number_value++;
        }  // for(auto &&e : columns_map) {

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
        keys.reserve(pk_columns.size());
        for (std::size_t position : pk_columns) {
            keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1]));
        }

        // build value metadata (yugawara::storage::index::column_ref)
        std::vector<yugawara::storage::index::column_ref> values;
        values.reserve(value_columns.size());
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
