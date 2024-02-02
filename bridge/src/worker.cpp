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

#include <vector>
#include <optional>
#include <string>
#include <chrono>

#include <boost/foreach.hpp>
#define BOOST_BIND_GLOBAL_PLACEHOLDERS  // to retain the current behavior
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

Worker::~Worker()
{
    if (transaction_handle_) {
        transaction_handle_.abort();
    }
}

ERROR_CODE Worker::begin_ddl(jogasaki::api::database& db)
{
    ERROR_CODE rv = ERROR_CODE::OK;

    if (!transaction_handle_) {
        jogasaki::api::transaction_option option{};
        option.modifies_definitions(true);
        if(auto rc = db.create_transaction(transaction_handle_, option); rc != jogasaki::status::ok) {  // FIXME specify exclusive exexution
            LOG(ERROR) << "fail to db.create_transaction(transaction_handle_) " << jogasaki::to_string_view(rc);
            rv = ERROR_CODE::UNKNOWN;
        }
    } else {
        rv = ERROR_CODE::TRANSACTION_ALREADY_STARTED;
    }
    VLOG(log_debug) << "<-- " << ogawayama::stub::error_name(rv);
    return rv;
}

ERROR_CODE Worker::end_ddl(jogasaki::api::database& db)
{
    ERROR_CODE rv = ERROR_CODE::OK;

    if (!transaction_handle_) {
        rv = ERROR_CODE::NO_TRANSACTION;
    } else {
        transaction_handle_.commit();
        if (auto rc = db.destroy_transaction(transaction_handle_); rc != jogasaki::status::ok) {
            LOG(ERROR) << "fail to db.destroy_transaction(transaction_handle_)";
            rv = ERROR_CODE::SERVER_FAILURE;
        }
        transaction_handle_ = jogasaki::api::transaction_handle();
    }
    VLOG(log_debug) << "<-- " << ogawayama::stub::error_name(rv);
    return rv;
}

static void get_data_length_vector(const boost::property_tree::ptree& column, std::vector<boost::optional<int64_t>>& data_length_vector) {
    boost::property_tree::ptree data_length_array = column.get_child(manager::metadata::Column::DATA_LENGTH);
    BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, data_length_array) {
        data_length_vector.emplace_back(node.second.get_value_optional<int64_t>());
    }
}

ERROR_CODE Worker::deploy_table(jogasaki::api::database& db, std::string_view str)
{
    std::istringstream ifs(std::string{str});
    boost::archive::binary_iarchive ia(ifs);
    return do_deploy_table(db, ia);
}

ERROR_CODE Worker::do_deploy_table(jogasaki::api::database& db, boost::archive::binary_iarchive& ia)
{
    std::size_t table_id;

    boost::property_tree::ptree table;
    ia >> table_id;
    ia >> table;
    {
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
            VLOG(log_debug) << "<-- INVALID_PARAMETER";
            return ERROR_CODE::INVALID_PARAMETER;
        }

        VLOG(log_debug) << " name is " << table_name.value();

        std::vector<std::size_t> pk_columns;  // index of primary key column: received order, content: column_number
        boost::optional<std::string> pk_columns_name;
        std::size_t unique_columns_n = 0;
        std::vector<std::vector<std::size_t>> unique_columns;  // index of unique constraint: received order, content: column_number
        std::vector<boost::optional<std::string>> unique_columns_name;
        std::vector<boost::optional<manager::metadata::ObjectIdType>> unique_index_id;
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
                pk_columns_name = constraint.get_optional<std::string>(manager::metadata::Object::NAME);
                break;
            case manager::metadata::Constraint::ConstraintType::UNIQUE:
                // Unique制約が設定されたカラムメタデータのカラム番号と名前を読み込む
                unique_index_id.resize(unique_columns_n + 1);
                unique_index_id.at(unique_columns_n) = constraint.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Constraint::INDEX_ID);
                unique_columns_name.resize(unique_columns_n + 1);
                unique_columns_name.at(unique_columns_n) = constraint.get_optional<std::string>(manager::metadata::Object::NAME);
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
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return ERROR_CODE::INVALID_PARAMETER;
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
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return ERROR_CODE::INVALID_PARAMETER;
            }
            const boost::property_tree::ptree& column = *e.second;
            
            auto is_not_null = column.get_optional<bool>(manager::metadata::Column::IS_NOT_NULL);
            auto data_type_id = column.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Column::DATA_TYPE_ID);
            auto name = column.get_optional<std::string>(manager::metadata::Column::NAME);
            auto column_number = column.get_optional<uint64_t>(manager::metadata::Column::COLUMN_NUMBER);
            if (!is_not_null || !data_type_id || !name) {
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return ERROR_CODE::INVALID_PARAMETER;
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
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return ERROR_CODE::INVALID_PARAMETER;  // IS_FUNCEXPR is necessary when default_expression is given
                }
                is_funcexpr = is_funcexpr_opt.value();
                if (is_funcexpr) {
                    LOG(ERROR) << " default value in function form is not currentry surported";
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return ERROR_CODE::UNSUPPORTED;  // default value in function form is not currentry surported
                }
            }

            if (auto itr = std::find(pk_columns.begin(), pk_columns.end(), column_number_value); itr != pk_columns.end()) {  // is this pk_column ?
                if(!is_not_null_value) {
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return ERROR_CODE::INVALID_PARAMETER;  // pk_column must be is_not_null
                }
            } else {  // this is value column
                value_columns.emplace_back(column_number_value);
            }
            VLOG(log_debug) << " found column: name = " << name_value <<                // NOLINT
                ", data_type_id = " << static_cast<std::size_t>(data_type_id_value) <<  // NOLINT
                ", is_not_null = " << (is_not_null_value ? "not_null" : "nullable"      // NOLINT
                );                                                                      // NOLINT

            switch(data_type_id_value) {  // build yugawara::storage::column
            case manager::metadata::DataTypes::DataTypesId::INT32:
            {
                // int column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::int32_t value = stoi(default_expression_value);
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
                    std::int64_t value = stol(default_expression_value);
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
                    float value = stof(default_expression_value);
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::float4>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float4(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::FLOAT64:
            {
                // double precision column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    double value = stod(default_expression_value);
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::float8>(value));
                }
                columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float8(), yugawara::variable::nullity(!is_not_null_value), default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::CHAR:
            case manager::metadata::DataTypes::DataTypesId::VARCHAR:
            {
                auto varying = column.get_optional<bool>(manager::metadata::Column::VARYING);
                if(!varying) {  // varying field is necessary for CHAR/VARCHRA
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";
                    return ERROR_CODE::INVALID_PARAMETER;
                }
                auto varying_value = varying.value();
                if((!varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::CHAR)) ||
                   (varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::VARCHAR))) {
                    VLOG(log_debug) << "<-- INVALID_PARAMETER";  // type and attributes are inconsistent
                    return ERROR_CODE::INVALID_PARAMETER;
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
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::character const>(default_expression_value.substr(1, index - 2)));
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
                std::optional<takatori::type::decimal::size_type> p{}, s{};
                std::vector<boost::optional<int64_t>> data_length_vector;
                get_data_length_vector(column, data_length_vector);
                if (data_length_vector.size() == 0) {
                    columns.emplace_back(yugawara::storage::column(name_value,
                                                                   takatori::type::decimal(),
                                                                   yugawara::variable::nullity(!is_not_null_value)));
                } else {
                    if (data_length_vector.size() > 0) {
                        if (auto po = data_length_vector.at(0); po) {
                            p = po.value();
                        }
                        if (data_length_vector.size() > 1) {
                            if (auto so = data_length_vector.at(1); so) {
                                s = so.value();
                            }
                        } else {
                            s = 0;
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
                // date column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind("::date");
                    if (index == std::string::npos) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    std::string dvs = default_expression_value.substr(1, index - 2);
                    struct tm tm;
                    memset(&tm, 0, sizeof(struct tm));
                    if (auto ts = strptime(dvs.c_str(), "%Y-%m-%d", &tm); ts == nullptr) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::date const>(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday));
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::date(),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIME:  // time_of_day
            {
                // time column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind("::time without time zone");
                    if (index == std::string::npos) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    std::string dvs = default_expression_value.substr(1, index - 2);
                    struct tm tm;
                    memset(&tm, 0, sizeof(struct tm));
                    if (auto ts = strptime(dvs.c_str(), "%H:%M:%S", &tm); ts != nullptr) {
                        std::string rem(ts);
                        default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::time_of_day const>(tm.tm_hour, tm.tm_min, tm.tm_sec, std::chrono::nanoseconds(!rem.empty() ? static_cast<std::size_t>(stod(rem) * 1000000000.) : 0)));
                    } else {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_of_day(~takatori::type::with_time_zone),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIMETZ:  // time_of_day
            {
                // time column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind("::time with time zone");
                    if (index == std::string::npos) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    std::string dvs = default_expression_value.substr(1, index - 2);
                    std::string dvs_t = dvs.substr(0, index - 5);
                    struct tm tm;
                    memset(&tm, 0, sizeof(struct tm));
                    if (auto ts = strptime(dvs_t.c_str(), "%H:%M:%S", &tm); ts != nullptr) {
                        auto td = stoi(dvs.substr(index - 5, index - 2));  // FIXME use time differnce value
                        VLOG(log_debug) << "  time difference is " << td;  // to suppress not use warning

                        std::string rem(ts);
                        default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::time_of_day const>(tm.tm_hour, tm.tm_min, tm.tm_sec, std::chrono::nanoseconds(!rem.empty() ? static_cast<std::size_t>(stod(rem) * 1000000000.) : 0)));
                    } else {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_of_day(takatori::type::with_time_zone),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIMESTAMP:  // time_point
            {
                // time column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind("::timestamp without time zone");
                    if (index == std::string::npos) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    std::string dvs = default_expression_value.substr(1, index - 2);
                    struct tm tm;
                    memset(&tm, 0, sizeof(struct tm));
                    if (auto ts = strptime(dvs.c_str(), "%Y-%m-%d %H:%M:%S", &tm); ts != nullptr) {
                        std::string rem(ts);
                        default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::time_point const>(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, std::chrono::nanoseconds(!rem.empty() ? static_cast<std::size_t>(stod(rem) * 1000000000.) : 0)));
                    } else {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_point(~takatori::type::with_time_zone),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
                break;
            }
            case manager::metadata::DataTypes::DataTypesId::TIMESTAMPTZ:  // time_point
            {
                // time column default value
                yugawara::storage::column_value default_value{};
                if (!default_expression_value.empty()) {
                    std::size_t index = default_expression_value.rfind("::timestamp with time zone");
                    if (index == std::string::npos) {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                    std::string dvs = default_expression_value.substr(1, index - 2);
                    std::string dvs_t = dvs.substr(0, index - 5);
                    struct tm tm;
                    memset(&tm, 0, sizeof(struct tm));
                    if (auto ts = strptime(dvs_t.c_str(), "%Y-%m-%d %H:%M:%S", &tm); ts != nullptr) {
                        auto td = stoi(dvs.substr(index - 5, index - 2));  // FIXME use time differnce value
                        VLOG(log_debug) << "  time difference is " << td;  // to suppress not use warning

                        std::string rem(ts);
                        default_value = yugawara::storage::column_value(std::make_shared<::takatori::value::time_point const>(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, std::chrono::nanoseconds(!rem.empty() ? static_cast<std::size_t>(stod(rem) * 1000000000.) : 0)));
                    } else {
                        VLOG(log_debug) << "<-- INVALID_PARAMETER";
                        return ERROR_CODE::INVALID_PARAMETER;
                    }
                }
                columns.emplace_back(yugawara::storage::column(name_value,
                                                               takatori::type::time_point(takatori::type::with_time_zone),
                                                               yugawara::variable::nullity(!is_not_null_value),
                                                               default_value));
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
        if (auto rc = db.create_table(t); rc != jogasaki::status::ok) {
            VLOG(log_debug) << "<-- " << ((rc == jogasaki::status::err_already_exists) ? "INVALID_PARAMETER" : "UNKNOWN");
            return (rc == jogasaki::status::err_already_exists) ? ERROR_CODE::INVALID_PARAMETER : ERROR_CODE::UNKNOWN;
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
        pn = table_name.value();

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
        if(db.create_index(i) != jogasaki::status::ok) {
            VLOG(log_debug) << "<-- UNKNOWN";
            return ERROR_CODE::UNKNOWN;
        }

        // secondary index from unique constraint
        for (std::size_t i = 0; i < unique_columns_n; i++) {
            auto& secondary_index_id = unique_index_id.at(i);
            auto& secondary_index = unique_columns.at(i);
            auto& secondary_index_name = unique_columns_name.at(i);

            if (!secondary_index_id) {
                VLOG(log_debug) << "<-- UNKNOWN";
                return ERROR_CODE::UNKNOWN;
            }

            // build key metadata (yugawara::storage::index::key)
            std::vector<yugawara::storage::index::key> si_keys;
            si_keys.clear();
            for (std::size_t position : secondary_index) {
                si_keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1]));
            }
            std::string in{};
            if (secondary_index_name) {
                in = secondary_index_name.value();
            } else {
                VLOG(log_debug) << "<-- INVALID_PARAMETER";
                return ERROR_CODE::INVALID_PARAMETER;
            }
            if (in.empty()) {
                in = table_name.value();
                in += "_constraint_";
                in += std::to_string(i);
            }

            auto si = std::make_shared<yugawara::storage::index>(
                std::make_optional(static_cast<yugawara::storage::index::definition_id_type>(static_cast<std::size_t>(secondary_index_id.value()))),
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
            if(db.create_index(si) != jogasaki::status::ok) {
                VLOG(log_debug) << "<-- UNKNOWN";
                return ERROR_CODE::UNKNOWN;
            }
        }

        VLOG(log_debug) << "<-- OK";
        return ERROR_CODE::OK;
    }
}

ERROR_CODE Worker::withdraw_table(jogasaki::api::database& db, std::string_view str)
{
    std::istringstream ifs(std::string{str});
    boost::archive::binary_iarchive ia(ifs);
    return do_withdraw_table(db, ia);
}

ERROR_CODE Worker::do_withdraw_table(jogasaki::api::database& db, boost::archive::binary_iarchive& ia)
{
    manager::metadata::ErrorCode error;
    std::size_t table_id;

    boost::property_tree::ptree table;
    ia >> table_id;
    ia >> table;

    VLOG(log_debug) << "found table with id " << table_id ;
    // table metadata
    auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
    auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
    if (!id || !table_name || (id.value() != table_id)) {
        VLOG(log_debug) << "<-- INVALID_PARAMETER";
        return ERROR_CODE::INVALID_PARAMETER;
    }

    VLOG(log_debug) << " name is " << table_name.value();
    if (auto rc = db.drop_index(table_name.value()); rc != jogasaki::status::ok) {
        VLOG(log_debug) << "<-- UNKNOWN";
        return ERROR_CODE::UNKNOWN;  // error in the server
    }
    if (auto rc = db.drop_table(table_name.value()); rc != jogasaki::status::ok) {
        VLOG(log_debug) << "<-- UNKNOWN";
        return ERROR_CODE::UNKNOWN;  // error in the server
    }
    VLOG(log_debug) << "<-- OK";
    return ERROR_CODE::OK;
}

ERROR_CODE Worker::do_deploy_index(jogasaki::api::database& db, boost::archive::binary_iarchive& ia)
{
    std::string table_name;
    boost::property_tree::ptree index;
    ia >> table_name;
    ia >> index;

    if (FLAGS_v >= log_debug) {
        std::ostringstream oss;
        boost::property_tree::json_parser::write_json(oss, index);
        VLOG(log_debug) << "==== index metadata for " << table_name << " begin ====";
        VLOG(log_debug) << oss.str();
        VLOG(log_debug) << "==== index metadata end ====";
    }

    if (auto table = db.find_table(table_name); table) {
        auto id_opt = index.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Object::ID);
        auto name_opt = index.get_optional<std::string>(manager::metadata::Index::NAME);
        std::vector<std::size_t> index_columns;
        if (id_opt && name_opt) {
            BOOST_FOREACH (const auto& column_node, index.get_child(manager::metadata::Index::KEYS)) {
                auto& index_column = column_node.second;
                auto column = index_column.get_value_optional<int64_t>();
                index_columns.emplace_back(column.value());
            }
            // build index key metadata (yugawara::storage::index::key)
            std::vector<yugawara::storage::index::key> si_keys;
            si_keys.clear();
            for (std::size_t position : index_columns) {
                si_keys.emplace_back(yugawara::storage::index::key(table->columns()[position-1]));
            }
            // index_feature_set
            bool is_unique{false};
            auto is_unique_opt = index.get_optional<bool>(manager::metadata::Index::IS_UNIQUE);
            if (is_unique_opt) {
                is_unique = is_unique_opt.value();
            }
            yugawara::storage::index_feature_set feature_set = 
                is_unique ? 
                yugawara::storage::index_feature_set{
                    ::yugawara::storage::index_feature::find,
                    ::yugawara::storage::index_feature::scan,
                    ::yugawara::storage::index_feature::unique,
                } :
                yugawara::storage::index_feature_set{
                    ::yugawara::storage::index_feature::find,
                    ::yugawara::storage::index_feature::scan,
                };
            auto si = std::make_shared<yugawara::storage::index>(
                std::make_optional(static_cast<yugawara::storage::index::definition_id_type>(id_opt.value())),
                table,
                yugawara::storage::index::simple_name_type(name_opt.value()),
                std::move(si_keys),
                std::move(std::vector<yugawara::storage::index::column_ref>{}),
                feature_set
            );
            if(db.create_index(si) != jogasaki::status::ok) {
                VLOG(log_debug) << "<-- UNKNOWN";
                return ERROR_CODE::UNKNOWN;  // error in the server
            }
            VLOG(log_debug) << "<-- OK";
            return ERROR_CODE::OK;
        }
        VLOG(log_debug) << "<-- INVALID_PARAMETER";
        return ERROR_CODE::INVALID_PARAMETER;  // id or name is not set in the ptree
    }
    VLOG(log_debug) << "<-- INVALID_PARAMETER";
    return ERROR_CODE::INVALID_PARAMETER;  // table not found
}

ERROR_CODE Worker::do_withdraw_index(jogasaki::api::database& db, boost::archive::binary_iarchive& ia)
{
    boost::property_tree::ptree index;
    ia >> index;

    if (auto name_opt = index.get_optional<std::string>(manager::metadata::Index::NAME); name_opt) {
        if (auto rc = db.drop_index(name_opt.value()); rc != jogasaki::status::ok) {
            VLOG(log_debug) << "<-- UNKNOWN";
            return ERROR_CODE::UNKNOWN;  // error in the server
        }
        VLOG(log_debug) << "<-- OK";
        return ERROR_CODE::OK;
    }
    VLOG(log_debug) << "<-- INVALID_PARAMETER";
    return ERROR_CODE::INVALID_PARAMETER;  // index not found
}

}  // ogawayama::bridge
