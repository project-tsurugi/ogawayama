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

boost::interprocess::managed_shared_memory managed_shared_memory;
std::unique_ptr<ogawayama::common::ChannelStream> chs;

void dummy_server(char const *name)
{
    boost::interprocess::shared_memory_object::remove(name);
    managed_shared_memory = boost::interprocess::managed_shared_memory(boost::interprocess::open_or_create, name, ogawayama::common::param::SEGMENT_SIZE);
    chs = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, &managed_shared_memory, true);
}



static StubPtr stub;
static ConnectionPtr connection;
static TransactionPtr transaction;
static MetadataPtr metadata;
static ResultSetPtr result_set;

int main() {
    //    dummy_server(ogawayama::common::param::server);

    stub = make_stub(ogawayama::common::param::SHARED_MEMORY_NAME);
    
    if (stub->get_connection(12, connection) != ogawayama::stub::ErrorCode::OK) { exit(1); }

    if (connection->begin(transaction) != ogawayama::stub::ErrorCode::OK) { exit(2); }

    if (transaction->execute_query("SELECT 1", result_set) != ogawayama::stub::ErrorCode::OK) { exit(3); }
    
    if (result_set->get_metadata(metadata) != ogawayama::stub::ErrorCode::OK) { exit(4); }

    while(true) {
        switch (result_set->next()) {
        case ogawayama::stub::ErrorCode::OK: {
            for (auto t: metadata->get_types()) {
                switch (t.get_type()) {
                case ogawayama::stub::Metadata::ColumnType::Type::INT16: {
                    std::int16_t v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::cout << v << std::endl;
                    break;
                }
                case ogawayama::stub::Metadata::ColumnType::Type::INT32: {
                    std::int32_t v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::cout << v << std::endl;
                    break;
                }
                case ogawayama::stub::Metadata::ColumnType::Type::INT64: {
                    std::int64_t v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::cout << v << std::endl;
                    break;
                }
                case ogawayama::stub::Metadata::ColumnType::Type::FLOAT32: {
                    float v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::cout << v << std::endl;
                    break;
                }
                case ogawayama::stub::Metadata::ColumnType::Type::FLOAT64: {
                    double v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::cout << v << std::endl;
                    break;
                }
                case ogawayama::stub::Metadata::ColumnType::Type::TEXT: {
                    std::string_view v;
                    if (result_set->next_column(v) != ogawayama::stub::ErrorCode::OK) { exit(5); }
                    std::size_t l = t.get_length();
                    std::cout << v << ":" << l << std::endl;
                    break;
                }
                default: {
                    exit(5);
                }
                }
            }
        }
        case ogawayama::stub::ErrorCode::END_OF_ROW: {
            break;
        }
        default: {
            exit(6);
        }
        }
    }

    return 0;
}
