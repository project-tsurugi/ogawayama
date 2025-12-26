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

static constexpr const char* name_prefix = "authentication_test";

class AuthenticationTest : public ::testing::Test {
    void SetUp() override {
        shm_name_ = std::string(name_prefix);
        shm_name_ += std::to_string(getpid());
        server_ = std::make_unique<server>(shm_name_, true);
    }
protected:
    std::unique_ptr<server> server_{};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes,misc-non-private-member-variables-in-classes)
    std::string shm_name_{};  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes,misc-non-private-member-variables-in-classes)
};

TEST_F(AuthenticationTest, ok) {  // NOLINT(google-readability-avoid-underscore-in-googletest-name)
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ogawayama::stub::Auth auth{"tsurugi", "password"};

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::OK, stub->get_connection(connection, 16, auth));
}

TEST_F(AuthenticationTest, error) {  // NOLINT(google-readability-avoid-underscore-in-googletest-name)
    StubPtr stub;
    ConnectionPtr connection;
    TransactionPtr transaction;
    ogawayama::stub::Auth auth{"tsurugi", "wordpass"};

    EXPECT_EQ(ERROR_CODE::OK, make_stub(stub, shm_name_));

    EXPECT_EQ(ERROR_CODE::AUTHENTICATION_ERROR, stub->get_connection(connection, 16, auth));
}

}  // namespace ogawayama::testing
