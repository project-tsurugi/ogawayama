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

class ResponseOnlyTest : public ::testing::Test {
    virtual void SetUp() {
        rv_ = system("if [ -f /dev/shm/tateyama-test ]; then rm -f /dev/shm/tateyama-test; fi ");
        wire_ = std::make_unique<tsubakuro::common::wire::server_wire_container>("tateyama-test");
    }
    virtual void TearDown() {
        rv_ = system("if [ -f /dev/shm/tateyama-test ]; then rm -f /dev/shm/tateyama-test; fi ");
    }

    int rv_;

public:
    static constexpr std::string_view request_test_message_ = "abcdefgh";
    static constexpr std::string_view response_test_message_ = "opqrstuvwxyz";
    static constexpr tsubakuro::common::wire::message_header::index_type index_ = 1;

    std::unique_ptr<tsubakuro::common::wire::server_wire_container> wire_;
    
    class test_service {
    public:
        int operator()(
            std::shared_ptr<tateyama::api::endpoint::request const> req,
            std::shared_ptr<tateyama::api::endpoint::response> res
        ) {
            auto payload = req->payload();
            EXPECT_EQ(request_test_message_, payload);
            res->body(response_test_message_);
            res->code(tateyama::api::endpoint::response_code::success);
            res->complete();
            return 0;
        }
    };

};

TEST_F(ResponseOnlyTest, normal) {
    auto& request_wire = wire_->get_request_wire();
    tsubakuro::common::wire::garbage_collector gc{};
    
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
    auto response = std::make_shared<tsubakuro::common::wire::ipc_response>(*request, h.get_idx(), gc);

    test_service sv;
    sv(static_cast<std::shared_ptr<tateyama::api::endpoint::request const>>(request),
             static_cast<std::shared_ptr<tateyama::api::endpoint::response>>(response));

    auto& r_box = wire_->get_response(h.get_idx());
    auto r_msg = r_box.recv();
    EXPECT_EQ(std::string_view(r_msg.first, r_msg.second), response_test_message_);
}

}  // namespace ogawayama::testing