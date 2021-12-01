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
#include "TestRoot.h"

namespace ogawayama::testing {

class ApiTest : public ::testing::Test {
    virtual void SetUp() {
        int pid;

        if (system("mkdir -p $HOME/.local/tsurugi/metadata") != 0) {
            std::cerr << "cannot mkdir" << std::endl;
        }
        if (system("cp ../../../stub/test/schema/tables.json $HOME/.local/tsurugi/metadata") != 0) {
            std::cerr << "cannot copy tables.json" << std::endl;
        }
        if (system("cp ../../../stub/test/schema/datatypes.json $HOME/.local/tsurugi/metadata") != 0) {
            std::cerr << "cannot copy datatypes.json" << std::endl;
        }
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

TEST_F(ApiTest, basic_flow) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->get_impl()->create_table(1));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO table_1 (column_1, column_2, column_3) VALUES (1.1, 'ABCDE', 1234)"
                                                            ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query("SELECT * FROM table_1", result_set));

    EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));

    auto& md = metadata->get_types();
    EXPECT_EQ(static_cast<std::size_t>(3), md.size());

    EXPECT_EQ(TYPE::FLOAT64, md.at(0).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(0).get_length());

    EXPECT_EQ(TYPE::TEXT, md.at(1).get_type());
//    EXPECT_EQ(static_cast<std::size_t>(8), md.at(1).get_length());  // text

    EXPECT_EQ(TYPE::INT64, md.at(2).get_type());
    EXPECT_EQ(static_cast<std::size_t>(8), md.at(2).get_length());
 
    double f;
    std::string_view t;
    std::int64_t b;
    EXPECT_EQ(ERROR_CODE::OK, result_set->next());
    
    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(f));
    EXPECT_EQ(static_cast<double>(1.1), f);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(t));
    EXPECT_EQ("ABCDE", t);

    EXPECT_EQ(ERROR_CODE::OK, result_set->next_column(b));
    EXPECT_EQ(static_cast<std::int64_t>(1234), b);

    EXPECT_EQ(ERROR_CODE::END_OF_ROW, result_set->next());

    EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
}

TEST_F(ApiTest, register_twice) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->get_impl()->create_table(1));

    EXPECT_EQ(ERROR_CODE::INVALID_PARAMETER, transaction->get_impl()->create_table(1));
}

TEST_F(ApiTest, nullable_pkey) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::INVALID_PARAMETER, transaction->get_impl()->create_table(2));
}

}  // namespace ogawayama::testing
