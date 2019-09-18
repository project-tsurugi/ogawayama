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

#include "tpcc_transaction.h"

#include <iostream>
#include "gflags/gflags.h"

#include "ogawayama/common/channel_stream.h"
#include "stubImpl.h"

DEFINE_bool(terminate, false, "terminate commnand");  // NOLINT
DEFINE_bool(dump, false, "Database contents are dumpd to the location just before shutdown");  //NOLINT

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

    stub = make_stub();
    
    if (stub->get_connection(12, connection) != ERROR_CODE::OK) { err_exit(__LINE__); }
    if (ogawayama::tpcc::tpcc_tables(connection.get()) != 0) { err_exit(__LINE__); }
    if (ogawayama::tpcc::tpcc_load(connection.get()) != 0) { err_exit(__LINE__); }
    connection = nullptr;  // disconnect before terminate the server
    
    if (FLAGS_dump) {
        stub->get_impl()->get_channel()->send(ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::DUMP_DATABASE, 0, ""));
    }
    if (FLAGS_terminate) {
        stub->get_impl()->get_channel()->send(ogawayama::common::CommandMessage(ogawayama::common::CommandMessage::Type::TERMINATE, 0, ""));
    }

    return 0;
}
