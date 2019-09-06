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

TEST_F(ApiTest, select_table) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);
    
    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(12, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

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
}


#if 0
    TEST_F(ApiTest, select_error) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);
    
    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(12, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::UNKNOWN, transaction->execute_query("SELECT C1, C2, C3 FROM DUMMY WHERE C1 <= 2", result_set));
}

TEST_F(ApiTest, statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);
    
    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(12, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement("UPDATE MOCK SET C2=3.3, C3='OPQRSTUVWXYZ' WHERE C1 = 3"));
              
    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

TEST_F(ApiTest, statement_error) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    stub = make_stub();
    EXPECT_NE(nullptr, stub);
    
    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(12, connection));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::UNKNOWN, transaction->execute_statement("UPDATE DUMMY SET C2=3.3, C3='OPQRSTUVWXYZ' WHERE C1 = 3"));
}
#endif

}  // namespace ogawayama::testing
