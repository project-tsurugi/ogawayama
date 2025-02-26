/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <gtest/gtest.h>

#include <set>

#include <jogasaki/proto/sql/response.pb.h>
#include "ogawayama/stub/search_path_adapter.h"
#include "ogawayama/stub/table_list_adapter.h"
#include "ogawayama/stub/table_metadata_adapter.h"
 
namespace ogawayama::testing {

static constexpr const char* name_prefix = "api_test";

class PathTest : public ::testing::Test {
    virtual void SetUp() {
        // list_tables_
        {
            auto* table_names = list_tables_.add_table_path_names();
            table_names->add_identifiers()->set_label("databaseName");
            table_names->add_identifiers()->set_label("schema1Name");
            table_names->add_identifiers()->set_label("table1Name");
        }
        {
            auto* table_names = list_tables_.add_table_path_names();
            table_names->add_identifiers()->set_label("databaseName");
            table_names->add_identifiers()->set_label("schema1Name");
            table_names->add_identifiers()->set_label("table2Name");
        }
        {
            auto* table_names = list_tables_.add_table_path_names();
            table_names->add_identifiers()->set_label("databaseName");
            table_names->add_identifiers()->set_label("schema2Name");
            table_names->add_identifiers()->set_label("table1Name");
        }
        {
            auto* table_names = list_tables_.add_table_path_names();
            table_names->add_identifiers()->set_label("databaseName");
            table_names->add_identifiers()->set_label("schema2Name");
            table_names->add_identifiers()->set_label("table2Name");
        }

        // search_path_
        {
            auto* search_paths = search_path_.add_search_paths();
            search_paths->add_identifiers()->set_label("databaseName");
            search_paths->add_identifiers()->set_label("schema1Name");
        }
        {
            auto* search_paths = search_path_.add_search_paths();
            search_paths->add_identifiers()->set_label("databaseName");
            search_paths->add_identifiers()->set_label("schema3Name");
        }
    }
protected:
    ::jogasaki::proto::sql::response::ListTables::Success list_tables_{};
    ::jogasaki::proto::sql::response::GetSearchPath::Success search_path_{};
};

TEST_F(PathTest, table_list_adapter) {
    std::set<std::string> expected{};
    expected.emplace("databaseName.schema1Name.table1Name");
    expected.emplace("databaseName.schema1Name.table2Name");
    expected.emplace("databaseName.schema2Name.table1Name");
    expected.emplace("databaseName.schema2Name.table2Name");
        
    auto tableListAdapter = std::make_unique<ogawayama::stub::table_list_adapter>(list_tables_);
    for (auto&& e: tableListAdapter->get_table_names()) {
        auto itr = expected.find(e);
        EXPECT_TRUE(itr != expected.end());
        expected.erase(itr);
    }
    EXPECT_TRUE(expected.empty());
}

TEST_F(PathTest, search_path_adapter) {
    std::set<std::string> expected{};
    expected.emplace("databaseName.schema1Name");
    expected.emplace("databaseName.schema3Name");
        
    auto searchPathAdapter = std::make_unique<ogawayama::stub::search_path_adapter>(search_path_);
    for (auto&& e: searchPathAdapter->get_schema_names()) {
        auto itr = expected.find(e);
        EXPECT_TRUE(itr != expected.end());
        expected.erase(itr);
    }
    EXPECT_TRUE(expected.empty());
}

TEST_F(PathTest, table_list_adapter_with_search_path_adapter) {
    std::set<std::string> expected{};
    expected.emplace("databaseName.schema1Name.table1Name");
    expected.emplace("databaseName.schema1Name.table2Name");
        
    auto tableListAdapter = std::make_unique<ogawayama::stub::table_list_adapter>(list_tables_);
    auto searchPathAdapter = std::make_unique<ogawayama::stub::search_path_adapter>(search_path_);
    for (auto&& e: tableListAdapter->get_simple_names(*searchPathAdapter)) {
        auto itr = expected.find(e);
        EXPECT_TRUE(itr != expected.end());
        expected.erase(itr);
    }
    EXPECT_TRUE(expected.empty());
}

}  // namespace ogawayama::testing
