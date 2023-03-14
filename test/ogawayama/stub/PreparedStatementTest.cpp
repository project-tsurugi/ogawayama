/*
 * Copyright 2018-2023 tsurugi project.
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

static constexpr const char* name_prefix = "prepared_statement_test";

class PreparedTest : public ::testing::Test {
    virtual void SetUp() {
        shm_name_ = std::string(name_prefix);
        shm_name_ += std::to_string(getpid());
        server_ = std::make_unique<server>(shm_name_);
    }
protected:
    std::unique_ptr<server> server_{};
    std::string shm_name_{};
};

TEST_F(PreparedTest, prepare) {
    StubPtr stub;
    ConnectionPtr connection;
    PreparedStatementPtr prepared_statement;
    TransactionPtr transaction;

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16));

    {
        jogasaki::proto::sql::response::Prepare rp{};
        auto ps = rp.mutable_prepared_statement_handle();
        ps->set_handle(1234);
        ps->set_has_result_records(false);
        server_->response_message(rp);
        rp.clear_prepared_statement_handle();

        ogawayama::stub::placeholders_type placeholders{};
        placeholders.emplace_back("int32_data:", ogawayama::stub::Metadata::ColumnType::Type::INT32);
        placeholders.emplace_back("int64_data:", ogawayama::stub::Metadata::ColumnType::Type::INT64);
        placeholders.emplace_back("float32_data:", ogawayama::stub::Metadata::ColumnType::Type::FLOAT32);
        placeholders.emplace_back("float64_data:", ogawayama::stub::Metadata::ColumnType::Type::FLOAT64);
        placeholders.emplace_back("text_data:", ogawayama::stub::Metadata::ColumnType::Type::TEXT);
        placeholders.emplace_back("decimal_data:", ogawayama::stub::Metadata::ColumnType::Type::DECIMAL);
        placeholders.emplace_back("date_data:", ogawayama::stub::Metadata::ColumnType::Type::DATE);
        placeholders.emplace_back("time_data:", ogawayama::stub::Metadata::ColumnType::Type::TIME);
        placeholders.emplace_back("timestamp_data:", ogawayama::stub::Metadata::ColumnType::Type::TIMESTAMP);
        placeholders.emplace_back("timetz_data:", ogawayama::stub::Metadata::ColumnType::Type::TIMETZ);
        placeholders.emplace_back("timestamptz_data:", ogawayama::stub::Metadata::ColumnType::Type::TIMESTAMPTZ);
        EXPECT_EQ(ERROR_CODE::OK, connection->prepare("this is a sql for the test", placeholders, prepared_statement));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), ::jogasaki::proto::sql::request::Request::RequestCase::kPrepare);

        auto prepare_request = request.prepare();
        EXPECT_EQ(prepare_request.sql(), "this is a sql for the test");
        EXPECT_EQ(prepare_request.placeholders_size(), 11);

        // 1st placeholder
        EXPECT_EQ(prepare_request.placeholders(0).name(), "int32_data:");
        EXPECT_EQ(prepare_request.placeholders(0).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(0).atom_type(), ::jogasaki::proto::sql::common::AtomType::INT4);
        // 2nd placeholder
        EXPECT_EQ(prepare_request.placeholders(1).name(), "int64_data:");
        EXPECT_EQ(prepare_request.placeholders(1).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(1).atom_type(), ::jogasaki::proto::sql::common::AtomType::INT8);
        // 3rd placeholder
        EXPECT_EQ(prepare_request.placeholders(2).name(), "float32_data:");
        EXPECT_EQ(prepare_request.placeholders(2).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(2).atom_type(), ::jogasaki::proto::sql::common::AtomType::FLOAT4);
        // 4th placeholder
        EXPECT_EQ(prepare_request.placeholders(3).name(), "float64_data:");
        EXPECT_EQ(prepare_request.placeholders(3).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(3).atom_type(), ::jogasaki::proto::sql::common::AtomType::FLOAT8);
        // 5th placeholder
        EXPECT_EQ(prepare_request.placeholders(4).name(), "text_data:");
        EXPECT_EQ(prepare_request.placeholders(4).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(4).atom_type(), ::jogasaki::proto::sql::common::AtomType::CHARACTER);
        // 6sh placeholder
        EXPECT_EQ(prepare_request.placeholders(5).name(), "decimal_data:");
        EXPECT_EQ(prepare_request.placeholders(5).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(5).atom_type(), ::jogasaki::proto::sql::common::AtomType::DECIMAL);
        // 7th placeholder
        EXPECT_EQ(prepare_request.placeholders(6).name(), "date_data:");
        EXPECT_EQ(prepare_request.placeholders(6).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(6).atom_type(), ::jogasaki::proto::sql::common::AtomType::DATE);
        // 8th placeholder
        EXPECT_EQ(prepare_request.placeholders(7).name(), "time_data:");
        EXPECT_EQ(prepare_request.placeholders(7).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(7).atom_type(), ::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY);
        // 9th placeholder
        EXPECT_EQ(prepare_request.placeholders(8).name(), "timestamp_data:");
        EXPECT_EQ(prepare_request.placeholders(8).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(8).atom_type(), ::jogasaki::proto::sql::common::AtomType::TIME_POINT);
        // 10th placeholder
        EXPECT_EQ(prepare_request.placeholders(9).name(), "timetz_data:");
        EXPECT_EQ(prepare_request.placeholders(9).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(9).atom_type(), ::jogasaki::proto::sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE);
        // 11th placeholder
        EXPECT_EQ(prepare_request.placeholders(10).name(), "timestamptz_data:");
        EXPECT_EQ(prepare_request.placeholders(10).type_info_case(), ::jogasaki::proto::sql::request::Placeholder::TypeInfoCase::kAtomType);
        EXPECT_EQ(prepare_request.placeholders(10).atom_type(), ::jogasaki::proto::sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE);
    }

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
        s.release_transaction_handle();
        s.release_transaction_id();
        b.release_success();

        EXPECT_EQ(ERROR_CODE::OK, connection->begin(transaction));

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kBegin);
        EXPECT_FALSE(request.begin().has_option());
    }

    {
        // build reply message for test
        jogasaki::proto::sql::response::ResultOnly ro{};
        jogasaki::proto::sql::response::Success s{};
        ro.set_allocated_success(&s);
        server_->response_message(ro);
        ro.release_success();

        // execute_statement()
        ogawayama::stub::parameters_type parameters{};
        parameters.emplace_back("int32_data:", static_cast<std::int32_t>(123456));
        parameters.emplace_back("int64_data:", static_cast<std::int64_t>(123456789));
        parameters.emplace_back("float32_data:", static_cast<float>(123.456));
        parameters.emplace_back("float64_data:", static_cast<double>(123.456789));
        parameters.emplace_back("text_data:", std::string("this is a string for the test"));
        auto date_for_test = takatori::datetime::date(2022, 12, 31);
        parameters.emplace_back("date_data:", date_for_test);
        auto time_of_day_for_test = takatori::datetime::time_of_day(23, 59, 59, std::chrono::nanoseconds(123457000));
        parameters.emplace_back("time_data:", time_of_day_for_test);
        auto time_point_for_test = takatori::datetime::time_point(takatori::datetime::date(2022, 12, 31), takatori::datetime::time_of_day(23, 59, 59, std::chrono::nanoseconds(987654000)));
        parameters.emplace_back("timestamp_data:", time_point_for_test);
        auto timetz_for_test = std::pair<takatori::datetime::time_of_day, std::int32_t>{time_of_day_for_test, 720};
        parameters.emplace_back("timetz_data:", timetz_for_test);
        auto time_pointtz_for_test = std::pair<takatori::datetime::time_point, std::int32_t>{time_point_for_test, 720};
        parameters.emplace_back("timestamptz_data:", time_pointtz_for_test);
        EXPECT_EQ(ERROR_CODE::OK, transaction->execute_statement(prepared_statement, parameters));

        // verify request message
        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kExecutePreparedStatement);

        auto eps_request = request.execute_prepared_statement();
        EXPECT_EQ(eps_request.prepared_statement_handle().handle(), 1234);
        EXPECT_EQ(eps_request.parameters_size(), 10);

        // 1st placeholder
        EXPECT_EQ(eps_request.parameters(0).name(), "int32_data:");
        EXPECT_EQ(eps_request.parameters(0).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kInt4Value);
        EXPECT_EQ(eps_request.parameters(0).int4_value(), 123456);
        // 2nd placeholder
        EXPECT_EQ(eps_request.parameters(1).name(), "int64_data:");
        EXPECT_EQ(eps_request.parameters(1).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kInt8Value);
        EXPECT_EQ(eps_request.parameters(1).int8_value(), 123456789);
        // 3rd placeholder
        EXPECT_EQ(eps_request.parameters(2).name(), "float32_data:");
        EXPECT_EQ(eps_request.parameters(2).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kFloat4Value);
        EXPECT_EQ(eps_request.parameters(2).float4_value(), static_cast<float>(123.456));
        // 4th placeholder
        EXPECT_EQ(eps_request.parameters(3).name(), "float64_data:");
        EXPECT_EQ(eps_request.parameters(3).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kFloat8Value);
        EXPECT_EQ(eps_request.parameters(3).float8_value(), static_cast<double>(123.456789));
        // 5th placeholder
        EXPECT_EQ(eps_request.parameters(4).name(), "text_data:");
        EXPECT_EQ(eps_request.parameters(4).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kCharacterValue);
        EXPECT_EQ(eps_request.parameters(4).character_value(), "this is a string for the test");
        // 6th placeholder
        EXPECT_EQ(eps_request.parameters(5).name(), "date_data:");
        EXPECT_EQ(eps_request.parameters(5).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kDateValue);
        EXPECT_EQ(eps_request.parameters(5).date_value(), date_for_test.days_since_epoch());
        // 7th placeholder
        EXPECT_EQ(eps_request.parameters(6).name(), "time_data:");
        EXPECT_EQ(eps_request.parameters(6).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kTimeOfDayValue);
        EXPECT_EQ(eps_request.parameters(6).time_of_day_value(), time_of_day_for_test.time_since_epoch().count());
        // 8th placeholder
        EXPECT_EQ(eps_request.parameters(7).name(), "timestamp_data:");
        EXPECT_EQ(eps_request.parameters(7).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kTimePointValue);
        EXPECT_EQ(eps_request.parameters(7).time_point_value().offset_seconds(), time_point_for_test.seconds_since_epoch().count());
        EXPECT_EQ(eps_request.parameters(7).time_point_value().nano_adjustment(), time_point_for_test.subsecond().count());
        // 9th placeholder
        EXPECT_EQ(eps_request.parameters(8).name(), "timetz_data:");
        EXPECT_EQ(eps_request.parameters(8).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kTimeOfDayWithTimeZoneValue);
        EXPECT_EQ(eps_request.parameters(8).time_of_day_with_time_zone_value().offset_nanoseconds(), time_of_day_for_test.time_since_epoch().count());
        EXPECT_EQ(eps_request.parameters(8).time_of_day_with_time_zone_value().time_zone_offset(), 720);
        // 10th placeholder
        EXPECT_EQ(eps_request.parameters(9).name(), "timestamptz_data:");
        EXPECT_EQ(eps_request.parameters(9).value_case(), ::jogasaki::proto::sql::request::Parameter::ValueCase::kTimePointWithTimeZoneValue);
        EXPECT_EQ(eps_request.parameters(9).time_point_with_time_zone_value().offset_seconds(), time_point_for_test.seconds_since_epoch().count());
        EXPECT_EQ(eps_request.parameters(9).time_point_with_time_zone_value().nano_adjustment(), time_point_for_test.subsecond().count());
        EXPECT_EQ(eps_request.parameters(9).time_point_with_time_zone_value().time_zone_offset(), 720);
    }

    {
        jogasaki::proto::sql::response::ResultOnly ro{};
        jogasaki::proto::sql::response::Success s{};
        ro.set_allocated_success(&s);
        server_->response_message(ro);
        EXPECT_EQ(ERROR_CODE::OK, transaction->commit());
        ro.release_success();

        std::optional<jogasaki::proto::sql::request::Request> request_opt = server_->request_message();
        EXPECT_TRUE(request_opt);
        auto request = request_opt.value();
        EXPECT_EQ(request.request_case(), jogasaki::proto::sql::request::Request::RequestCase::kCommit);
    }
}

}  // namespace ogawayama::testing
