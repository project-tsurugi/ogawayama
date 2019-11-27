/*
 * Copyright 2018-2019 tsurugi project.
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
#include "TestRoot.h"

namespace ogawayama::testing {

class OwnerTimeoutTest : public ::testing::Test {
    virtual void SetUp() {
        if (fork() == 0) { // Backend, soon exit.
            close(0); close(1); close(2);
            auto shared_memory = std::make_unique<ogawayama::common::SharedMemory>("ogawayama", true);
            sleep(2);
            {
                auto server_channel = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory.get());
                auto row_queue = std::make_unique<ogawayama::common::RowQueue>(shared_memory->shm_name(ogawayama::common::param::resultset, 12, 3).c_str(), shared_memory.get());
                sleep(2);
            }
            exit(0);
        } else { // Frontend, continue to timeout test.
            sleep(1);
            shared_memory_ = std::make_unique<ogawayama::common::SharedMemory>("ogawayama");
            server_channel_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory_.get(), true);
            row_queue_ = std::make_unique<ogawayama::common::RowQueue>(shared_memory_->shm_name(ogawayama::common::param::resultset, 12, 3).c_str(), shared_memory_.get(), true);
        }
    }

    virtual void TearDown() {
    }

protected:
    std::unique_ptr<ogawayama::common::SharedMemory> shared_memory_;
    std::unique_ptr<ogawayama::common::ChannelStream> server_channel_;
    std::unique_ptr<ogawayama::common::RowQueue> row_queue_;
};

TEST_F(OwnerTimeoutTest, channel) {
    ERROR_CODE reply = server_channel_->recv_ack();

    EXPECT_EQ(ERROR_CODE::SERVER_FAILURE, reply);
}

TEST_F(OwnerTimeoutTest, row_queue) {
    ERROR_CODE reply = row_queue_->next();

    EXPECT_EQ(ERROR_CODE::SERVER_FAILURE, reply);
}

}  // namespace ogawayama::testing
