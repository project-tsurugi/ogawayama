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

#include <iostream>

#include "ogawayama/common/channel_stream.h"
#include "ogawayama/stub/api.h"

static StubPtr stub;
static ConnectionPtr connection;
static TransactionPtr transaction;

void err_exit(int line)
{
    std::cerr << "Error at " << line << std::endl;
    exit(1);
}

int main() {
    stub = make_stub();
    
    if (stub->get_connection(12, connection) != ERROR_CODE::OK) { err_exit(__LINE__); }

    if (connection->begin(transaction) != ERROR_CODE::OK) { err_exit(__LINE__); }

    if (transaction->execute_statement("CREATE TABLE T2 ("
                                       "C1 INT NOT NULL PRIMARY KEY, "
                                       "C2 DOUBLE NOT NULL, "
                                       "C3 CHAR(5) NOT NULL,"
                                       "C4 INT, "
                                       "C5 BIGINT, "
                                       "C6 FLOAT, "
                                       "C7 VARCHAR(5)"
                                       ")")
        != ERROR_CODE::OK) { err_exit(__LINE__); }
    
    if (transaction->execute_statement("INSERT INTO T2 (C1, C2, C3) VALUES(1, 1.1, 'ABCDE')")
        != ERROR_CODE::OK) { err_exit(__LINE__); }

    transaction->commit();
    return 0;
}
