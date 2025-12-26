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
#include <unistd.h>
#include <boost/property_tree/ptree.hpp>

#include <jogasaki/serializer/value_output.h>

#include "stub_test_root.h"


namespace ogawayama::testing {

static constexpr const char* name_prefix = "error_test";

class ErrorTest : public ::testing::Test {
    void SetUp() override {
        shm_name_ = std::string(name_prefix);
        shm_name_ += std::to_string(getpid());
        server_ = std::make_unique<server>(shm_name_);
    }
protected:
    std::unique_ptr<server> server_{};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes,misc-non-private-member-variables-in-classes)
    std::string shm_name_{};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes,misc-non-private-member-variables-in-classes)

    StubPtr stub_{};
    ConnectionPtr connection_{};
    PreparedStatementPtr prepared_statement_{};
    TransactionPtr transaction_{};
    ResultSetPtr result_set_{};
    MetadataPtr metadata_{};

    void begin() {
        jogasaki::proto::sql::response::Begin b{};
        auto* s = b.mutable_success();
        auto* t = s->mutable_transaction_handle();
        t->set_handle(0x12345678);
        auto* tid = s->mutable_transaction_id();
        tid->set_id("transaction_id_for_test");
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::OK, connection_->begin(transaction_));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }

    void commit(bool success = true) {
        jogasaki::proto::sql::response::ResultOnly roc{};
        jogasaki::proto::sql::response::ResultOnly rod{};

        if (success) {
            (void) roc.mutable_success();
            server_->response_message(roc);

            (void) rod.mutable_success();
            server_->response_message(rod);

            EXPECT_EQ(ERROR_CODE::OK, transaction_->commit());
        } else {
            auto* e = roc.mutable_error();
            e->set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
            e->set_detail("sql_error due to permission error");
            server_->response_message(roc);

            (void) rod.mutable_success();
            server_->response_message(rod);

            EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction_->commit());
            ogawayama::stub::tsurugi_error_code code{};
            EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));
            EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        }

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);

        std::optional<jogasaki::proto::sql::request::Request> requestd_opt = server_->request_message();
        EXPECT_TRUE(requestd_opt);
        auto requestd = requestd_opt.value();
        EXPECT_EQ(requestd.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kDisposeTransaction);
    }
};

TEST_F(ErrorTest, begin) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    {
        jogasaki::proto::sql::response::Begin b{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        b.set_allocated_error(&e);
        server_->response_message(b);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection_->begin(transaction_));
        (void) b.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, prepare) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

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
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection_->prepare("insert into table (c1, c2) values(1, 23)", placeholders, prepared_statement_));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), ::jogasaki::proto::sql::request::Request::RequestCase::kPrepare);
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, statement) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

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
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction_->execute_statement("INSERT INTO TABLE (C1, C2) VALUES(1, 2)", num_rows));
        (void) er.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteStatement);
        EXPECT_EQ(request.execute_statement().sql(), "INSERT INTO TABLE (C1, C2) VALUES(1, 2)");
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }

    commit();

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, query) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

    {
		// body
        jogasaki::proto::sql::response::ResultOnly ro{};
        jogasaki::proto::sql::response::Error e{};
        e.set_code(::jogasaki::proto::sql::error::Code::SQL_SERVICE_EXCEPTION);
        e.set_detail("sql_error_for_test");
        e.set_supplemental_text("supplemental_text_for_test");
        ro.set_allocated_error(&e);
        server_->response_message(ro);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction_->execute_query("SELECT * FROM T2", result_set_));
        (void) ro.release_error();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::sql_error);
        EXPECT_EQ(code.code, 1000); // 1000 corresponds to SQL_SERVICE_EXCEPTION
        EXPECT_EQ(code.name, "SQL_SERVICE_EXCEPTION");
        EXPECT_EQ(code.detail, "sql_error_for_test");
        EXPECT_EQ(code.supplemental_text, "supplemental_text_for_test");
    }

    commit();

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, query_permission_error) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

    {
        // body
        auto code = tateyama::proto::diagnostics::Code::PERMISSION_ERROR;  // implementation constraints
        server_->response_message(code);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction_->execute_query("SELECT * FROM T2", result_set_));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::framework_error);
        EXPECT_EQ(code.code, 202); // 202 stands for PERMISSION_ERROR (see ogawayama/transport/tsurugi_error.h)
        EXPECT_EQ(code.name, "request is not permitted.");
    }

    commit(false);

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, query_body_head_permission_error) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

    {
        // metadata
        jogasaki::proto::sql::response::ResultSetMetadata m{};
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::INT4);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::CHARACTER);
        server_->response_body_head(m);
        // body
        auto code = tateyama::proto::diagnostics::Code::PERMISSION_ERROR;  // implementation constraints
        server_->response_message(code);
        EXPECT_EQ(ERROR_CODE::OK, transaction_->execute_query("SELECT * FROM T2", result_set_));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");

        EXPECT_EQ(ERROR_CODE::OK, result_set_->get_metadata(metadata_));

        try {
            std::int32_t i{};
            EXPECT_EQ(ERROR_CODE::SERVER_ERROR, result_set_->next_column(i));
        } catch (std::runtime_error &ex) {
            FAIL();
        }
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::framework_error);
        EXPECT_EQ(code.code, 202); // 202 stands for PERMISSION_ERROR (see ogawayama/transport/tsurugi_error.h)
        EXPECT_EQ(code.name, "request is not permitted.");
    }

    commit(false);

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, query_body_head_permission_error_next) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

    {
        // metadata
        jogasaki::proto::sql::response::ResultSetMetadata m{};
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::INT4);
        m.add_columns()->set_atom_type(jogasaki::proto::sql::common::AtomType::CHARACTER);
        server_->response_body_head(m);
        // body
        auto code = tateyama::proto::diagnostics::Code::PERMISSION_ERROR;  // implementation constraints
        server_->response_message(code);
        EXPECT_EQ(ERROR_CODE::OK, transaction_->execute_query("SELECT * FROM T2", result_set_));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecuteQuery);
        EXPECT_EQ(request.execute_query().sql(), "SELECT * FROM T2");

        EXPECT_EQ(ERROR_CODE::OK, result_set_->get_metadata(metadata_));

        try {
            EXPECT_EQ(ERROR_CODE::SERVER_ERROR, result_set_->next());
        } catch (std::runtime_error &ex) {
            FAIL();
        }
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::framework_error);
        EXPECT_EQ(code.code, 202); // 202 stands for PERMISSION_ERROR (see ogawayama/transport/tsurugi_error.h)
        EXPECT_EQ(code.name, "request is not permitted.");
    }

    commit(false);

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, commit) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    begin();

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

        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, transaction_->commit());
        (void) roc.release_error();
        (void) rod.release_success();

        std::optional<jogasaki::proto::sql::request::Request> requestc_opt = server_->request_message();
        EXPECT_TRUE(requestc_opt);
        auto requestc = requestc_opt.value();
        EXPECT_EQ(requestc.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));

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

    EXPECT_TRUE(server_->is_response_empty());
}

TEST_F(ErrorTest, cancel) {
    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub_, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub_->get_connection(connection_, 16));

    {
        auto code = tateyama::proto::diagnostics::Code::OPERATION_CANCELED;  // implementation constraints
        server_->response_message(code);
        EXPECT_EQ(ERROR_CODE::SERVER_ERROR, connection_->begin(transaction_));
    }
    {
        ogawayama::stub::tsurugi_error_code code{};
        EXPECT_EQ(ERROR_CODE::OK, connection_->tsurugi_error(code));
        EXPECT_EQ(code.type, ogawayama::stub::tsurugi_error_code::tsurugi_error_type::framework_error);
        EXPECT_EQ(code.code, 403);  // SCD-00403 is OPERATION_CANCELED
        EXPECT_EQ(code.name, "operation was canceled by user or system.");
    }

    EXPECT_TRUE(server_->is_response_empty());
}

}  // namespace ogawayama::testing
