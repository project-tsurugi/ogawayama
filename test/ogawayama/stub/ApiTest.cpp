/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <boost/property_tree/ptree.hpp>

#include <jogasaki/serializer/value_output.h>

#include "stub_test_root.h"


namespace ogawayama::testing {

static constexpr const char* name_prefix = "api_test";

class ApiTest : public ::testing::Test {
    virtual void SetUp() {
        shm_name_ = std::string(name_prefix);
        shm_name_ += std::to_string(getpid());
        server_ = std::make_unique<server>(shm_name_);
    }
protected:
    std::unique_ptr<server> server_{};
    std::string shm_name_{};
};

TEST_F(ApiTest, begin_commit) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Begin::Success s{};
        jogasaki::proto::sql::common::Transaction t{};
        jogasaki::proto::sql::common::TransactionId tid{};
        tid.set_id("transaction_id_for_test");
        t.set_handle(0x12345678);
        s.set_allocated_transaction_handle(&t);
        s.set_allocated_transaction_id(&tid);
        b.set_allocated_success(&s);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));
        (void) s.release_transaction_handle();
        (void) s.release_transaction_id();
        (void) b.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod, 1);

        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        (void) roc.release_success();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

TEST_F(ApiTest, long_transaction) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        boost::property_tree::ptree option;
        boost::property_tree::ptree wp;
        boost::property_tree::ptree tbl1;
        boost::property_tree::ptree tbl2;
        tbl1.put(ogawayama::stub::TABLE_NAME, "table1");
        tbl2.put(ogawayama::stub::TABLE_NAME, "table2");
        wp.push_back(boost::property_tree::ptree::value_type("", tbl1));
        wp.push_back(boost::property_tree::ptree::value_type("", tbl2));
        option.put_child(ogawayama::stub::WRITE_PRESERVE, wp);
        option.put(ogawayama::stub::TRANSACTION_TYPE, ogawayama::stub::TransactionType::LONG);
        option.put(ogawayama::stub::TRANSACTION_PRIORITY, ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        option.put(ogawayama::stub::TRANSACTION_LABEL, "this is a transaction_label the test");
        
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Begin::Success s{};
        jogasaki::proto::sql::common::Transaction t{};
        jogasaki::proto::sql::common::TransactionId tid{};
        tid.set_id("transaction_id_for_test");
        t.set_handle(0x12345678);
        s.set_allocated_transaction_handle(&t);
        s.set_allocated_transaction_id(&tid);
        b.set_allocated_success(&s);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection->begin(option, transaction));
        (void) s.release_transaction_handle();
        (void) s.release_transaction_id();
        (void) b.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_TRUE(request.begin().has_option());
        auto transaction_option = request.begin().option();
        EXPECT_EQ(transaction_option.type(), ogawayama::stub::TransactionType::LONG);
        EXPECT_EQ(transaction_option.priority(), ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        EXPECT_EQ(transaction_option.label(), "this is a transaction_label the test");
        auto write_preserves = transaction_option.write_preserves();
        EXPECT_EQ(write_preserves.size(), 2);
        bool first = true;
        for(auto&& write_preserve : write_preserves) {
            EXPECT_EQ(write_preserve.table_name(), first ? "table1" : "table2");
            first = false;
        }
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod, 1);

        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        (void) roc.release_success();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

TEST_F(ApiTest, long_transaction_inclusive_read_area) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        boost::property_tree::ptree option;
        boost::property_tree::ptree ra;
        boost::property_tree::ptree tbl1;
        boost::property_tree::ptree tbl2;
        tbl1.put(ogawayama::stub::TABLE_NAME, "table1");
        tbl2.put(ogawayama::stub::TABLE_NAME, "table2");
        ra.push_back(boost::property_tree::ptree::value_type("", tbl1));
        ra.push_back(boost::property_tree::ptree::value_type("", tbl2));
        option.put_child(ogawayama::stub::INCLUSIVE_READ_AREA, ra);
        option.put(ogawayama::stub::TRANSACTION_TYPE, ogawayama::stub::TransactionType::LONG);
        option.put(ogawayama::stub::TRANSACTION_PRIORITY, ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        option.put(ogawayama::stub::TRANSACTION_LABEL, "this is a transaction_label the test");
        
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Begin::Success s{};
        jogasaki::proto::sql::common::Transaction t{};
        jogasaki::proto::sql::common::TransactionId tid{};
        tid.set_id("transaction_id_for_test");
        t.set_handle(0x12345678);
        s.set_allocated_transaction_handle(&t);
        s.set_allocated_transaction_id(&tid);
        b.set_allocated_success(&s);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection->begin(option, transaction));
        (void) s.release_transaction_handle();
        (void) s.release_transaction_id();
        (void) b.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_TRUE(request.begin().has_option());
        auto transaction_option = request.begin().option();
        EXPECT_EQ(transaction_option.type(), ogawayama::stub::TransactionType::LONG);
        EXPECT_EQ(transaction_option.priority(), ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        EXPECT_EQ(transaction_option.label(), "this is a transaction_label the test");
        auto inclusive_read_areas = transaction_option.inclusive_read_areas();
        EXPECT_EQ(inclusive_read_areas.size(), 2);
        bool first = true;
        for(auto&& inclusive_read_area : inclusive_read_areas) {
            EXPECT_EQ(inclusive_read_area.table_name(), first ? "table1" : "table2");
            first = false;
        }
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod, 1);

        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        (void) roc.release_success();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

TEST_F(ApiTest, long_transaction_exclusive_read_area) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        boost::property_tree::ptree option;
        boost::property_tree::ptree ra;
        boost::property_tree::ptree tbl1;
        boost::property_tree::ptree tbl2;
        tbl1.put(ogawayama::stub::TABLE_NAME, "table1");
        tbl2.put(ogawayama::stub::TABLE_NAME, "table2");
        ra.push_back(boost::property_tree::ptree::value_type("", tbl1));
        ra.push_back(boost::property_tree::ptree::value_type("", tbl2));
        option.put_child(ogawayama::stub::EXCLUSIVE_READ_AREA, ra);
        option.put(ogawayama::stub::TRANSACTION_TYPE, ogawayama::stub::TransactionType::LONG);
        option.put(ogawayama::stub::TRANSACTION_PRIORITY, ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        option.put(ogawayama::stub::TRANSACTION_LABEL, "this is a transaction_label the test");
        
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Begin::Success s{};
        jogasaki::proto::sql::common::Transaction t{};
        jogasaki::proto::sql::common::TransactionId tid{};
        tid.set_id("transaction_id_for_test");
        t.set_handle(0x12345678);
        s.set_allocated_transaction_handle(&t);
        s.set_allocated_transaction_id(&tid);
        b.set_allocated_success(&s);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection->begin(option, transaction));
        (void) s.release_transaction_handle();
        (void) s.release_transaction_id();
        (void) b.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_TRUE(request.begin().has_option());
        auto transaction_option = request.begin().option();
        EXPECT_EQ(transaction_option.type(), ogawayama::stub::TransactionType::LONG);
        EXPECT_EQ(transaction_option.priority(), ogawayama::stub::TransactionPriority::INTERRUPT_EXCLUDE);
        EXPECT_EQ(transaction_option.label(), "this is a transaction_label the test");
        auto exclusive_read_areas = transaction_option.exclusive_read_areas();
        EXPECT_EQ(exclusive_read_areas.size(), 2);
        bool first = true;
        for(auto&& exclusive_read_area : exclusive_read_areas) {
            EXPECT_EQ(exclusive_read_area.table_name(), first ? "table1" : "table2");
            first = false;
        }
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod, 1);

        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        (void) roc.release_success();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

TEST_F(ApiTest, result_set) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;
    MetadataPtr metadata;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Begin::Success s{};
        jogasaki::proto::sql::common::Transaction t{};
        jogasaki::proto::sql::common::TransactionId tid{};
        tid.set_id("transaction_id_for_test");
        t.set_handle(0x12345678);
        s.set_allocated_transaction_handle(&t);
        s.set_allocated_transaction_id(&tid);
        b.set_allocated_success(&s);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));
        (void) s.release_transaction_handle();
        (void) s.release_transaction_id();
        (void) b.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }

    {
        // metadata
        jogasaki::proto::sql::response::ResultSetMetadata m{};
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::INT4);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::FLOAT8);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::CHARACTER);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::INT4);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::INT8);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::FLOAT4);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::CHARACTER);
        // resultset
        std::queue<std::string> resultset{};
        std::string row{};
        row.resize(8196);  // enough to write
        takatori::util::buffer_view buf { row.data(), row.size() };
        takatori::util::buffer_view::iterator iter = buf.begin();
        auto end = buf.end();
        jogasaki::serializer::write_row_begin(7, iter, end);
        jogasaki::serializer::write_int(1, iter, end);
        jogasaki::serializer::write_float8(1.1, iter, end);
        jogasaki::serializer::write_character("ABCDE", iter, end);
        jogasaki::serializer::write_null(iter, end);
        jogasaki::serializer::write_null(iter, end);
        jogasaki::serializer::write_null(iter, end);
        jogasaki::serializer::write_null(iter, end);
        jogasaki::serializer::write_end_of_contents(iter, end);
        row.resize(std::distance(buf.begin(), iter));
        resultset.emplace(row);
		// body
        jogasaki::proto::sql::response::ResultOnly ro{};
        jogasaki::proto::sql::response::Success s{};
        ro.set_allocated_success(&s);
        // set response
        server_->response_with_resultset(m, resultset, ro);
        // clear fields
        (void) ro.release_success();
        m.clear_columns();

        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_query("SELECT * FROM T2", result_set));

        EXPECT_EQ(ERROR_CODE::OK, result_set->get_metadata(metadata));

        auto& md = metadata->get_types();
        EXPECT_EQ(static_cast<std::size_t>(7), md.size());

        EXPECT_EQ(TYPE::INT32, md.at(0).get_type());

        EXPECT_EQ(TYPE::FLOAT64, md.at(1).get_type());

        EXPECT_EQ(TYPE::TEXT, md.at(2).get_type());

        EXPECT_EQ(TYPE::INT32, md.at(3).get_type());

        EXPECT_EQ(TYPE::INT64, md.at(4).get_type());

        EXPECT_EQ(TYPE::FLOAT32, md.at(5).get_type());

        EXPECT_EQ(TYPE::TEXT, md.at(6).get_type());

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

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");
    }

    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod, 1);

        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        (void) roc.release_success();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

}  // namespace ogawayama::testing
