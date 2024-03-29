/*
 * Copyright 2019-2013 Project Tsurugi.
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
#include "tpcc_transaction.h"

#include <iostream>
#include "gflags/gflags.h"

#include "ogawayama/common/channel_stream.h"
#include "stubImpl.h"

DEFINE_bool(generate, false, "Database contents are randomly generateed and loadded into the database");  //NOLINT
DEFINE_uint32(warehouse, 0, "TPC-C Scale factor.");  //NOLINT

static StubPtr stub;
static ConnectionPtr connection;

void err_exit(int line)
{
    std::cerr << "Error at " << line << std::endl;
    exit(1);
}

int main(int argc, char **argv) {
    // command arguments
    gflags::SetUsageMessage("ogawayama database server");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (make_stub(stub) != ERROR_CODE::OK) { err_exit(__LINE__); }
    if (stub->get_connection(connection, 12) != ERROR_CODE::OK) { err_exit(__LINE__); }
    if (ogawayama::tpcc::tpcc_tables(connection.get()) != 0) { err_exit(__LINE__); }
    if (FLAGS_generate) {
        if (FLAGS_warehouse > 0) {
            ogawayama::tpcc::normal.warehouses = FLAGS_warehouse;
            ogawayama::tpcc::scale = &ogawayama::tpcc::normal;
        }
        if (ogawayama::tpcc::tpcc_load(connection.get()) != 0) { err_exit(__LINE__); }
    }
    connection = nullptr;  // disconnect before terminate the server

    return 0;
}
