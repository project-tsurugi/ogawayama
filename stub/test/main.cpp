/*
 * Copyright 2018-2018 umikongo project.
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

#include <errno.h>

int main(int argc, char** argv) {
    // first consume command line options for gtest
    ::testing::InitGoogleTest(&argc, argv);

    int pid;

    if ((pid = fork()) == 0) {  // child process
        auto retv = execlp("server/src/ogawayama-server", "ogawayama-server", nullptr);
        if (retv != 0) perror("error in ogawayama-server ");
        _exit(0);
    }
    sleep(1);

    StubPtr stub;
    stub = make_stub();

    ConnectionPtr connection;
    stub->get_connection(11, connection);
    TransactionPtr transaction;
    connection->begin(transaction);
    transaction->execute_statement(
                "CREATE TABLE T2 ("
                "C1 INT NOT NULL PRIMARY KEY, "
                "C2 DOUBLE NOT NULL, "
                "C3 CHAR(5) NOT NULL,"
                "C4 INT, "
                "C5 BIGINT, "
                "C6 FLOAT, "
                "C7 VARCHAR(5)"
                ")"
                                   );
    transaction->execute_statement(
                "INSERT INTO T2 (C1, C2, C3) VALUES(1, 1.1, 'ABCDE')"
                                   );
    transaction->commit();
    transaction = nullptr;
    connection = nullptr;
    
    auto retv = RUN_ALL_TESTS();

    stub->get_impl()->get_channel()->get_binary_oarchive() <<
        ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::TERMINATE);
    return retv;
}
