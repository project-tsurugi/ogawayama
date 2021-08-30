/*
 * Copyright 2018-2021 tsurugi project.
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

namespace ogawayama::testing {

class ResultSetTest : public ::testing::Test {
    virtual void SetUp() {
        rv_ = system("if [ -f /dev/shm/tateyama-test ]; then rm -f /dev/shm/tateyama-test; fi ");
        wire_ = std::make_unique<tsubakuro::common::wire::server_wire_container>("tateyama-test");
    }
    virtual void TearDown() {
        rv_ = system("if [ -f /dev/shm/tateyama-test ]; then rm -f /dev/shm/tateyama-test*; fi ");
    }

    int rv_;

public:
    static constexpr std::string_view request_test_message_ = "abcdefgh";
    static constexpr tsubakuro::common::wire::message_header::index_type index_ = 1;
    static constexpr std::string_view r11_ = "row_11_data";
    static constexpr std::string_view r12_ = "row_12_data";
    static constexpr std::string_view r21_ = "row_21_data";
    static constexpr std::string_view r22_ = "row_22_data";

    std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire_;
    
    class test_service {
    public:
        int operator()(
            std::shared_ptr<tateyama::api::endpoint::request const> req,
            std::shared_ptr<tateyama::api::endpoint::response> res
        ) {
            auto payload = req->payload();
            EXPECT_EQ(request_test_message_, payload);
            res->body(resultset_wire_name_);
            res->code(tateyama::api::endpoint::response_code::started);
            res->complete();

            tateyama::api::endpoint::data_channel* dc;
            EXPECT_EQ(res->acquire_channel(resultset_wire_name_, dc), tateyama::status::ok);
            tateyama::api::endpoint::writer* w;
            EXPECT_EQ(dc->acquire(w), tateyama::status::ok);            

            w->write(r11_.data(), r11_.length());
            w->write(r12_.data(), r12_.length());
            w->commit();
            w->write(r21_.data(), r21_.length());
            w->write(r22_.data(), r22_.length());
            w->commit();
            w->commit();

            res->code(tateyama::api::endpoint::response_code::success);
            EXPECT_EQ(dc->release(*w), tateyama::status::ok);
            EXPECT_EQ(res->release_channel(*dc), tateyama::status::ok);

            return 0;
        }

    private:
        static constexpr std::string_view resultset_wire_name_ = "resultset_1";
    };
};

TEST_F(ResultSetTest, normal) {
    auto& request_wire = wire_->get_request_wire();
    
    request_wire.write(request_test_message_.data(),
                       tsubakuro::common::wire::message_header(index_, request_test_message_.length()));

    auto h = request_wire.peep(true);
    EXPECT_EQ(index_, h.get_idx());
    auto length = h.get_length();
    std::string message;
    message.resize(length);
    memcpy(message.data(), request_wire.payload(length), length);
    EXPECT_EQ(message, request_test_message_);

    auto request = std::make_shared<tsubakuro::common::wire::ipc_request>(*wire_, h);
    auto response = std::make_shared<tsubakuro::common::wire::ipc_response>(*request, h.get_idx());

    test_service sv;
    sv(static_cast<std::shared_ptr<tateyama::api::endpoint::request const>>(request),
             static_cast<std::shared_ptr<tateyama::api::endpoint::response>>(response));

    auto& r_box = wire_->get_response(h.get_idx());
    auto r_msg = r_box.recv();
    auto resultset_wires =
        wire_->create_resultset_wires_for_client(std::string_view(r_msg.first, r_msg.second));

    auto chunk_1 = resultset_wires->get_chunk();
    std::string r1(r11_);
    r1 += r12_;
    EXPECT_EQ(r1, std::string_view(chunk_1.first, chunk_1.second));
    resultset_wires->dispose(r1.length());

    auto chunk_2 = resultset_wires->get_chunk();
    std::string r2(r21_);
    r2 += r22_;
    EXPECT_EQ(r2, std::string_view(chunk_2.first, chunk_2.second));
    resultset_wires->dispose(r2.length());

    auto chunk_3 = resultset_wires->get_chunk();
    EXPECT_EQ(chunk_3.second, 0);
    EXPECT_TRUE(resultset_wires->is_eor());
}

}  // namespace ogawayama::testing
