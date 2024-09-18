/*
 * Copyright 2018-2024 Project Tsurugi.
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

static constexpr const char* name_prefix = "error_test";

class ErrorTest : public ::testing::Test {
    virtual void SetUp() {
        shm_name_ = std::string(name_prefix);
        shm_name_ += std::to_string(getpid());
        server_ = std::make_unique<server>(shm_name_);
    }
protected:
    std::unique_ptr<server> server_{};
    std::string shm_name_{};
};

TEST_F(ErrorTest, begin) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        b.set_allocated_error(&e);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection->begin(transaction));
        (void) b.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }
}

TEST_F(ErrorTest, prepare) {
    StubPtr stub;
    ConnectionPtr connection;
    PreparedStatementPtr prepared_statement;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        jogasaki::proto::sql::response::Prepare rp{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        rp.set_allocated_error(&e);
        server_->response_message(rp);
        (void) rp.release_error();

        ogawayama::stub::placeholders_type placeholders{};
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection->prepare("insert into table (c1, c2) values(1, 23)", placeholders, prepared_statement));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), ::jogasaki::proto::sql::request::Request::RequestCase::kPrepare);
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }
}

TEST_F(ErrorTest, statement) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;

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
		// body
        jogasaki::proto::sql::response::ExecuteResult er{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        er.set_allocated_error(&e);
        server_->response_message(er);
        std::size_t num_rows{};
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction->execute_statement("INSERT INTO TABLE (C1, C2) VALUES(1, 2)", num_rows));
        (void) er.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteStatement);
        EXPECT_EQ(request.execute_statement().sql(), "INSERT INTO TABLE (C1, C2) VALUES(1, 2)");
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod);

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

TEST_F(ErrorTest, query) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ResultSetPtr result_set;

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
		// body
        jogasaki::proto::sql::response::ResultOnly ro{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        ro.set_allocated_error(&e);
        server_->response_message(ro);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction->execute_query("SELECT * FROM T2", result_set));
        (void) ro.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }
    {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::Success sc{};
        roc.set_allocated_success(&sc);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod);

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

TEST_F(ErrorTest, commit) {
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
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        roc.set_allocated_error(&e);
        server_->response_message(roc);

        jogasaki::proto::sql::response::ResultOnly rod{};
        jogasaki::proto::sql::response::Success sd{};
        rod.set_allocated_success(&sd);
        server_->response_message(rod);

        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction->commit());
        (void) roc.release_error();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }
    {
        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
}

TEST_F(ErrorTest, framework) {
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        ::tateyama::proto::framework::response::Header h{};
        h.set_payload_type(::tateyama::proto::framework::response::Header_PayloadType::Header_PayloadType_SERVER_DIAGNOSTICS);
        server_->response_message(h);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection->begin(transaction));
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection->tsurugi_error(code));
        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::framework_error);
    }
}

}  // namespace ogawayama::testing
