/*
 * Copyright 2018-2019 umikongo project.
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
#include "TestRoot.h"

namespace ogawayama::testing {

class ApiTest : public ::testing::Test {};

TEST_F(ApiTest, use_executable_statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(12, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "CREATE TABLE T2 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL,"
                                                             "C4 INT, "
                                                             "C5 BIGINT, "
                                                             "C6 FLOAT, "
                                                             "C7 VARCHAR(5)"
                                                             ")"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T2 (C1, C2, C3) VALUES(1, 1.1, 'ABCDE')"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query("SELECT * FROM T2", result_set));

    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));

    auto md = metadata->get_types();
    EXPECT_EQ(static_cast<std::size_t>(7), md.size());

    EXPECT_EQ(TYPE::INT32, md.at(0).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(0).get_length());

    EXPECT_EQ(TYPE::FLOAT64, md.at(1).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(1).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(2).get_type());
    EXPECT_EQ(static_cast<std::size_t>(5), md.at(2).get_length());

    EXPECT_EQ(TYPE::INT32, md.at(3).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(3).get_length());

    EXPECT_EQ(TYPE::INT64, md.at(4).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(4).get_length());

    EXPECT_EQ(TYPE::FLOAT32, md.at(5).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(5).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(6).get_type());
    EXPECT_EQ(static_cast<std::size_t>(5), md.at(6).get_length());
 
    std::int32_t i;
    std::int64_t b;
    float f;
    double d;
    std::string_view t;
    EXPECT_EQ(ERROR_CODE::OK, result_set->next());
    
    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(i));
    EXPECT_EQ(static_cast<std::int32_t>(1), i);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(d));
    EXPECT_EQ(static_cast<double>(1.1), d);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(t));
    EXPECT_EQ("ABCDE", t);

    EXPECT_EQ(ERROR_CODE::COLUMN_WAS_NULL, result_set->next_column(i));

    EXPECT_EQ(ERROR_CODE::COLUMN_WAS_NULL, result_set->next_column(b));

    EXPECT_EQ(ERROR_CODE::COLUMN_WAS_NULL, result_set->next_column(f));

    EXPECT_EQ(ERROR_CODE::COLUMN_WAS_NULL, result_set->next_column(t));

    EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set->next());

    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement("DROP TABLE T2"));
    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

TEST_F(ApiTest, mixing_executable_statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set1;
    ResultSetPtr result_set2;
    MetadataPtr metadata1;
    MetadataPtr metadata2;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(13, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "CREATE TABLE T3 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL"
                                                             ")"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T3 (C1, C2, C3) VALUES(1, 1.1, '11111')"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T3 (C1, C2, C3) VALUES(2, 2.2, '22222')"
                                                             ));

    auto query1 = "SELECT * FROM T3 WHERE C1 = 1";
    auto query2 = "SELECT * FROM T3 WHERE C1 = 2";

    for(int idx=0; idx < 2; ++idx) {
        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(query1, result_set1));
        EXPECT_EQ(ERROR_CODE::OK, result_set1->get_metadata(metadata1));
        auto md1 = metadata1->get_types();
        EXPECT_EQ(static_cast<std::size_t>(3), md1.size());
        EXPECT_EQ(TYPE::INT32, md1.at(0).get_type());
        EXPECT_EQ(static_cast<std::size_t>(4), md1.at(0).get_length());
        EXPECT_EQ(TYPE::FLOAT64, md1.at(1).get_type());
        EXPECT_EQ(static_cast<std::size_t>(8), md1.at(1).get_length());
        EXPECT_EQ(TYPE::TEXT, md1.at(2).get_type());
        EXPECT_EQ(static_cast<std::size_t>(5), md1.at(2).get_length());

        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(query2, result_set2));
        EXPECT_EQ(ERROR_CODE::OK, result_set2->get_metadata(metadata2));
        auto md2 = metadata1->get_types();
        EXPECT_EQ(static_cast<std::size_t>(3), md2.size());
        EXPECT_EQ(TYPE::INT32, md2.at(0).get_type());
        EXPECT_EQ(static_cast<std::size_t>(4), md2.at(0).get_length());
        EXPECT_EQ(TYPE::FLOAT64, md2.at(1).get_type());
        EXPECT_EQ(static_cast<std::size_t>(8), md2.at(1).get_length());
        EXPECT_EQ(TYPE::TEXT, md2.at(2).get_type());
        EXPECT_EQ(static_cast<std::size_t>(5), md2.at(2).get_length());

        std::int32_t i;
        double d;
        std::string_view t;

        EXPECT_EQ(ERROR_CODE::OK, result_set1->next());

        EXPECT_EQ(ERROR_CODE::OK, result_set1->next_column(i));
        EXPECT_EQ(static_cast<std::int32_t>(1), i);

        EXPECT_EQ(ERROR_CODE::OK, result_set1->next_column(d));
        EXPECT_EQ(static_cast<double>(1.1), d);

        EXPECT_EQ(ERROR_CODE::OK, result_set1->next_column(t));
        EXPECT_EQ("11111", t);

        EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set1->next());


        EXPECT_EQ(ERROR_CODE::OK, result_set2->next());

        EXPECT_EQ(ERROR_CODE::OK, result_set2->next_column(i));
        EXPECT_EQ(static_cast<std::int32_t>(2), i);

        EXPECT_EQ(ERROR_CODE::OK, result_set2->next_column(d));
        EXPECT_EQ(static_cast<double>(2.2), d);

        EXPECT_EQ(ERROR_CODE::OK, result_set2->next_column(t));
        EXPECT_EQ("22222", t);

        EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set2->next());
    }

    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement("DROP TABLE T3"));
    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

//
// ogawayama does not supprt "mixing_transactions"
//
// TEST_F(NewApiTest, mixing_transactions) {
// }

TEST_F(ApiTest, fetch_metadata) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(14, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "CREATE TABLE T5 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL,"
                                                             "C4 INT, "
                                                             "C5 BIGINT, "
                                                             "C6 FLOAT, "
                                                             "C7 VARCHAR(5),"
                                                             "C8 CHAR(1)"
                                                             ")"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T5 (C1, C2, C3, C4, C5, C6, C7) VALUES(1, 1.1, 'ABCDE', 2, 3, 4.4, 'XYZZZ')"
                                                             ));

    auto query = "SELECT C1+C1, C2+C2, C3||C3, C1+C2, C1+C4, C1+C5, C2+C6, C3||C7, C8 FROM T5";
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(query, result_set));
    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));
    auto md = metadata->get_types();
    EXPECT_EQ(static_cast<std::size_t>(9), md.size());

    EXPECT_EQ(TYPE::INT32, md.at(0).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(0).get_length());

    EXPECT_EQ(TYPE::FLOAT64, md.at(1).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(1).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(2).get_type());
    //    EXPECT_EQ(static_cast<std::size_t>(0), md.at(2).get_length());

    EXPECT_EQ(TYPE::FLOAT64, md.at(3).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(3).get_length());

    EXPECT_EQ(TYPE::INT32, md.at(4).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(4).get_length());

    EXPECT_EQ(TYPE::INT64, md.at(5).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(5).get_length());

    EXPECT_EQ(TYPE::FLOAT64, md.at(6).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(6).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(7).get_type());
    //    EXPECT_EQ(static_cast<std::size_t>(0), md.at(7).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(8).get_type());
    EXPECT_EQ(static_cast<std::size_t>(1), md.at(8).get_length());

    std::int32_t i;
    std::int64_t b;
    double d;
    std::string_view t;
    EXPECT_EQ(ERROR_CODE::OK, result_set->next());

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(i));
    EXPECT_EQ(static_cast<std::int32_t>(2), i);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(d));
    EXPECT_DOUBLE_EQ(2.2, d);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(t));
    EXPECT_EQ("ABCDEABCDE", t);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(d));
    EXPECT_DOUBLE_EQ(2.1, d);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(i));
    EXPECT_EQ(static_cast<std::int32_t>(3), i);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(b));
    EXPECT_EQ(static_cast<std::int64_t>(4), b);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(d));
    EXPECT_FLOAT_EQ(5.5, d);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(t));
    EXPECT_EQ("ABCDEXYZZZ", t);

    EXPECT_EQ(ERROR_CODE::COLUMN_WAS_NULL, result_set->next_column(t));

    EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set->next());

    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());


    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement("DROP TABLE T5"));
    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

}  // namespace ogawayama::testing
