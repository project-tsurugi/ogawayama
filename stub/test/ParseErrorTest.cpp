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

class ParseErrorTest : public ::testing::Test {
    virtual void SetUp() {
        envelope_ = std::make_unique<ogawayama::bridge::envelope>();
    }
private:
    std::unique_ptr<ogawayama::bridge::envelope> envelope_{};
};

TEST_F(ParseErrorTest, DISABLED_execute_statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "CREATE TABLE T1 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL"
                                                             ")"
                                                             ));
    
    EXPECT_EQ(ERROR_CODE::UNKNOWN, transaction->execute_statement(
                                                             "INSERT INTO T1 (C1, C2, C4) VALUES(1, 1.1, '11111')"
                                                             ));

}

TEST_F(ParseErrorTest, DISABLED_execute_query) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 12));

    EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "CREATE TABLE T1 ("
                                                             "C1 INT NOT NULL PRIMARY KEY, "
                                                             "C2 DOUBLE NOT NULL, "
                                                             "C3 CHAR(5) NOT NULL"
                                                             ")"
                                                             ));
    
    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T1 (C1, C2, C3) VALUES(1, 1.1, '11111')"
                                                             ));

    EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(
                                                             "INSERT INTO T1 (C1, C2, C3) VALUES(2, 2.2, '22222')"
                                                             ));

    EXPECT_EQ(ERROR_CODE::UNKNOWN, transaction->execute_query("SELECT * FROM T1 WHERE C4 = 0", result_set));
}

}  // namespace ogawayama::testing
