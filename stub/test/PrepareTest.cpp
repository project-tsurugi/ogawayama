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
#include "TestRoot.h"

namespace ogawayama::testing {

class PrepareTest : public ::testing::Test {
    virtual void SetUp() {
        int pid;

        if ((pid = fork()) == 0) {  // child process, execute only at build/stub/test directory.
            auto retv = execlp("../../server/src/ogawayama-server", "ogawayama-server", "-remove_shm", nullptr);
            if (retv != 0) perror("error in ogawayama-server ");
            _exit(0);
        }
        sleep(1);
    }

    virtual void TearDown() {
        StubPtr stub;
        EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));
        stub->get_impl()->send_terminate();
    }
};

TEST_F(PrepareTest, use_executable_statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_create_table(
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
    PreparedStatementPtr prepared_insert;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "INSERT INTO T2 (C1, C2, C3, C4) VALUES(:p1, :p2, :p3, :p4)", prepared_insert));
    
    prepared_insert->set_parameter(1);
    prepared_insert->set_parameter(1.1);
    prepared_insert->set_parameter("ABCDE");
    prepared_insert->set_parameter();
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_insert.get()));

    PreparedStatementPtr prepared_query;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "SELECT * FROM T2", prepared_query));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(prepared_query.get(), result_set));

    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));

    auto& md = metadata->get_types();
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
}

TEST_F(PrepareTest, mixing_executable_statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set1;
    ResultSetPtr result_set2;
    MetadataPtr metadata1;
    MetadataPtr metadata2;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 13));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_create_table(
                                                             "CREATE TABLE T3 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL"
                                                             ")"
                                                             ));

    PreparedStatementPtr prepared_insert;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "INSERT INTO T3 (C1, C2, C3) VALUES(:p1, :p2, :p3)", prepared_insert));

    prepared_insert->set_parameter(1);
    prepared_insert->set_parameter(1.1);
    prepared_insert->set_parameter("11111");
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_insert.get()));

    prepared_insert->set_parameter(2);
    prepared_insert->set_parameter(2.2);
    prepared_insert->set_parameter("22222");
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_insert.get()));

    PreparedStatementPtr prepared_query1;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "SELECT * FROM T3 WHERE C1 = 1", prepared_query1));
    PreparedStatementPtr prepared_query2;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "SELECT * FROM T3 WHERE C1 = 2", prepared_query2));

    for(int idx=0; idx < 2; ++idx) {
        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(prepared_query1.get(), result_set1));
        EXPECT_EQ(ERROR_CODE::OK, result_set1->get_metadata(metadata1));
        auto& md1 = metadata1->get_types();
        EXPECT_EQ(static_cast<std::size_t>(3), md1.size());
        EXPECT_EQ(TYPE::INT32, md1.at(0).get_type());
        EXPECT_EQ(static_cast<std::size_t>(4), md1.at(0).get_length());
        EXPECT_EQ(TYPE::FLOAT64, md1.at(1).get_type());
        EXPECT_EQ(static_cast<std::size_t>(8), md1.at(1).get_length());
        EXPECT_EQ(TYPE::TEXT, md1.at(2).get_type());
        EXPECT_EQ(static_cast<std::size_t>(5), md1.at(2).get_length());

        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(prepared_query2.get(), result_set2));
        EXPECT_EQ(ERROR_CODE::OK, result_set2->get_metadata(metadata2));
        auto& md2 = metadata1->get_types();
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
}

//
// ogawayama does not supprt "mixing_transactions"
//
// TEST_F(PrepareTest, mixing_transactions) {
// }

TEST_F(PrepareTest, fetch_metadata) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 14));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_create_table(
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

    PreparedStatementPtr prepared_insert;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "INSERT INTO T5 (C1, C2, C3, C4, C5, C6, C7) VALUES(:p1, :p2, :p3, :p4, :p5, :p6, :p7)", prepared_insert));
    
    prepared_insert->set_parameter(1);
    prepared_insert->set_parameter(1.1);
    prepared_insert->set_parameter("ABCDE");
    prepared_insert->set_parameter(2);
    prepared_insert->set_parameter(3);
    prepared_insert->set_parameter(4.4);
    prepared_insert->set_parameter("XYZZZ");
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_insert.get()));

    PreparedStatementPtr prepared_query;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "SELECT C1+C1, C2+C2, C3||C3, C1+C2, C1+C4, C1+C5, C2+C6, C3||C7, C8 FROM T5", prepared_query));
    
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(prepared_query.get(), result_set));
    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));
    auto& md = metadata->get_types();
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
}

TEST_F(PrepareTest, passing_multiple_row) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;
    const std::int32_t limit = 123; // Not a multiple of 32

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 15));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_create_table(
                                                             "CREATE TABLE T5 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL"
                                                             ")"
                                                             ));

    PreparedStatementPtr prepared_insert;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "INSERT INTO T5 (C1, C2) VALUES(:p1, :p2)", prepared_insert));

    for(std::int32_t idx=0; idx < limit; ++idx) {
        prepared_insert->set_parameter(idx);
        prepared_insert->set_parameter((idx*10+idx) / 10.0);
        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_insert.get()));
    }

    PreparedStatementPtr prepared_query;
    EXPECT_EQ(ERROR_CODE::OK, connection->prepare( "SELECT * FROM T5 ORDER by C1", prepared_query));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query(prepared_query.get(), result_set));
    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));
    auto& md = metadata->get_types();
    EXPECT_EQ(static_cast<std::size_t>(2), md.size());
    EXPECT_EQ(TYPE::INT32, md.at(0).get_type());
    EXPECT_EQ(static_cast<std::size_t>(4), md.at(0).get_length());
    EXPECT_EQ(TYPE::FLOAT64, md.at(1).get_type());

    for(std::int32_t idx=0; idx < limit; ++idx) {
        std::int32_t i;
        double d;

        EXPECT_EQ(ERROR_CODE::OK, result_set->next());

        EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(i));
        EXPECT_EQ(idx, i);

        EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(d));
        EXPECT_EQ((double)(idx*10+idx) / 10.0, d);
    }
    EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set->next());

    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

}  // namespace ogawayama::testing
