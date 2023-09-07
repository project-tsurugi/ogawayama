/*
 * Copyright 2018-2019 tsurugi project.
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
#include <unistd.h>
#include <stdlib.h>
#include <exception>

#include "gtest/gtest.h"

#define BOOST_BIND_GLOBAL_PLACEHOLDERS
#include <boost/property_tree/json_parser.hpp>
#include <boost/archive/basic_archive.hpp> // need for ubuntu 20.04
#include <boost/property_tree/ptree_serialization.hpp>
#include <boost/archive/binary_oarchive.hpp>

#include <takatori/value/boolean.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/type/character.h>
#include <takatori/value/character.h>
#include <takatori/value/octet.h>
#include <takatori/value/decimal.h>
#include <takatori/value/date.h>
#include <takatori/value/time_of_day.h>
#include <takatori/value/time_point.h>

#include "worker.h"
#include "database_mock.h"

namespace ogawayama::testing {

class SchemaDeployTest : public ::testing::Test {
    virtual void SetUp() {
        database_mock_ = std::make_unique<database_mock>();
    }
    virtual void TearDown() {
    }

protected:
    std::unique_ptr<database_mock> database_mock_{};
};

TEST_F(SchemaDeployTest, basic_flow) {
    std::size_t table_id{1};
    boost::property_tree::ptree table_ptree;
    try {
        boost::property_tree::read_json("../../test/ogawayama/bridge/schema/tables.json", table_ptree);
    } catch (std::exception& ex) {
        FAIL() << "error in read_json() of tables.json";
    }

    std::ostringstream ofs;
    boost::archive::binary_oarchive oa(ofs);
    oa << table_id;
    oa << table_ptree;

    EXPECT_EQ(ogawayama::bridge::Worker::deploy_metadata(*database_mock_, ofs.str()), ERROR_CODE::OK);
    auto& table = database_mock_->get_table();
    EXPECT_EQ(table->simple_name(), "table_for_test");
    auto& columns = table->columns();
    EXPECT_EQ(columns.size(), 10);
    // column 0
    auto& c0 = columns.at(0);
    EXPECT_EQ(c0.simple_name(), "k0");
    EXPECT_EQ(c0.type().kind(), takatori::type::type_kind::int8);
    EXPECT_EQ(c0.criteria().nullity(), yugawara::variable::nullity(false));
    // column 1
    auto& c1 = columns.at(1);
    EXPECT_EQ(c1.simple_name(), "c0");
    EXPECT_EQ(c1.type().kind(), takatori::type::type_kind::int4);
    EXPECT_EQ(static_cast<takatori::value::int4 const&>(*(c1.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              123);
    // column 2
    auto& c2 = columns.at(2);
    EXPECT_EQ(c2.simple_name(), "c1");
    EXPECT_EQ(c2.type().kind(), takatori::type::type_kind::int8);
    EXPECT_EQ(static_cast<takatori::value::int8 const&>(*(c2.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              123456789);
    EXPECT_EQ(c2.criteria().nullity(), yugawara::variable::nullity(true));
    // column 3
    auto& c3 = columns.at(3);
    EXPECT_EQ(c3.simple_name(), "c2");
    EXPECT_EQ(c3.type().kind(), takatori::type::type_kind::float4);
    EXPECT_EQ(static_cast<takatori::value::float4 const&>(*(c3.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              static_cast<float>(123.456));
    EXPECT_EQ(c3.criteria().nullity(), yugawara::variable::nullity(true));
    // column 4
    auto& c4 = columns.at(4);
    EXPECT_EQ(c4.simple_name(), "c3");
    EXPECT_EQ(c4.type().kind(), takatori::type::type_kind::float8);
    EXPECT_EQ(static_cast<takatori::value::float8 const&>(*(c4.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              static_cast<double>(123456.789));
    EXPECT_EQ(c4.criteria().nullity(), yugawara::variable::nullity(true));
    // column 5
    auto& c5 = columns.at(5);
    EXPECT_EQ(c5.simple_name(), "c4");
    EXPECT_EQ(c5.type().kind(), takatori::type::type_kind::character);
    EXPECT_EQ(static_cast<takatori::value::character const&>(*(c5.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              "fixed length default");
    EXPECT_EQ(c5.criteria().nullity(), yugawara::variable::nullity(true));
    auto& c5c = static_cast<takatori::type::character const&>(c5.type());
    EXPECT_EQ(c5c.varying(), false);
    auto c5l = c5c.length();
    EXPECT_TRUE(c5l);
    EXPECT_EQ(c5l.value(), 30);
    // column 6
    auto& c6 = columns.at(6);
    EXPECT_EQ(c6.simple_name(), "c5");
    EXPECT_EQ(c6.type().kind(), takatori::type::type_kind::character);
    EXPECT_EQ(static_cast<takatori::value::character const&>(*(c6.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              "variable length default");
    EXPECT_EQ(c6.criteria().nullity(), yugawara::variable::nullity(true));
    auto& c6c = static_cast<takatori::type::character const&>(c6.type());
    EXPECT_EQ(c6c.varying(), true);
    auto c6l = c6c.length();
    EXPECT_TRUE(c6l);
    EXPECT_EQ(c6l.value(), 30);
    // column 7
    auto& c7 = columns.at(7);
    EXPECT_EQ(c7.simple_name(), "date_immediate");
    EXPECT_EQ(c7.type().kind(), takatori::type::type_kind::date);
    EXPECT_EQ(static_cast<takatori::value::date const&>(*(c7.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              takatori::datetime::date(2022, 12, 31));
    EXPECT_EQ(c7.criteria().nullity(), yugawara::variable::nullity(true));
    // column 8
    auto& c8 = columns.at(8);
    EXPECT_EQ(c8.simple_name(), "time_immediate");
    EXPECT_EQ(c8.type().kind(), takatori::type::type_kind::time_of_day);
    EXPECT_EQ(static_cast<takatori::value::time_of_day const&>(*(c8.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              takatori::datetime::time_of_day(23, 59, 59, std::chrono::nanoseconds(123457000)));
    EXPECT_EQ(c8.criteria().nullity(), yugawara::variable::nullity(true));
    // column 9
    auto& c9 = columns.at(9);
    EXPECT_EQ(c9.simple_name(), "timestamp_immediate");
    EXPECT_EQ(c9.type().kind(), takatori::type::type_kind::time_point);
    EXPECT_EQ(static_cast<takatori::value::time_point const&>(*(c9.default_value().element<yugawara::storage::column_value_kind::immediate>())).get(),
              takatori::datetime::time_point(takatori::datetime::date(2022, 12, 31), takatori::datetime::time_of_day(23, 59, 59, std::chrono::nanoseconds(987654000))));
    EXPECT_EQ(c9.criteria().nullity(), yugawara::variable::nullity(true));

    // index
    auto& indexes = database_mock_->get_indexes();
    EXPECT_EQ(indexes.size(), 1);
    // keys in the index
    auto& keys = indexes.at(0)->keys();
    EXPECT_EQ(keys.size(), 1);
    EXPECT_EQ(keys.at(0), c0);
    // values in the index
    auto& values = indexes.at(0)->values();
    EXPECT_EQ(values.size(), 9);
    EXPECT_EQ(values.at(0), c1);
    EXPECT_EQ(values.at(1), c2);
    EXPECT_EQ(values.at(2), c3);
    EXPECT_EQ(values.at(3), c4);
    EXPECT_EQ(values.at(4), c5);
    EXPECT_EQ(values.at(5), c6);
    EXPECT_EQ(values.at(6), c7);
    EXPECT_EQ(values.at(7), c8);
    EXPECT_EQ(values.at(8), c9);
}

}  // namespace ogawayama::testing
