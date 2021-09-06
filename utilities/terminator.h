/*
 * Copyright 2019-2021 tsurugi project.
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
#pragma once

#include "wire.h"

namespace tsubakuro::common::wire {

class connection_container
{
    static constexpr std::size_t request_queue_size = (1<<12);  // 4K bytes (tentative)

public:
    connection_container(std::string_view db_name) : db_name_(db_name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            connection_queue_ = managed_shared_memory_->find<connection_queue>(connection_queue::name).first;
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            throw std::runtime_error("cannot find a database with the specified name");
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_container(connection_container const&) = delete;
    connection_container(connection_container&&) = delete;
    connection_container& operator = (connection_container const&) = delete;
    connection_container& operator = (connection_container&&) = delete;

    connection_queue& get_connection_queue() {
        return *connection_queue_;
    }
    
private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    connection_queue* connection_queue_;
};

};  // namespace tsubakuro::common::wire
