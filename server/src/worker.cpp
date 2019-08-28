/*
 * Copyright 2019-2019 tsurugi project.
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

#include <iostream>

#include "ogawayama/common/row_queue.h"
#include "backend.h"

namespace ogawayama::server {

void worker_main(ogawayama::common::SharedMemory *shared_memory_ptr, std::int32_t id)
{
    boost::interprocess::managed_shared_memory *managed_shared_memory_ptr = shared_memory_ptr->get_managed_shared_memory_ptr();
    auto request = std::make_unique<ogawayama::common::ChannelStream>(shared_memory_ptr->shm_name(ogawayama::common::param::request, id).c_str(), managed_shared_memory_ptr);
    auto result = std::make_unique<ogawayama::common::ChannelStream>(shared_memory_ptr->shm_name(ogawayama::common::param::result, id).c_str(), managed_shared_memory_ptr);
    result->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK);
    
    while(true) {
        ogawayama::common::ChannelMessage request_message;
        try {
            request->get_binary_iarchive() >> request_message;
            std::cerr << __func__ << " " << __LINE__ << ": recieved normally" << std::endl;
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return;
        }
        
        switch (request_message.get_type()) {
        case ogawayama::common::ChannelMessage::Type::EXECUTE_STATEMENT:
            result->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK, 1);

            std::cerr << request_message.get_string() << std::endl;
            
            break;
        case ogawayama::common::ChannelMessage::Type::EXECUTE_QUERY:
            {
                ogawayama::stub::Metadata metadata;
                metadata.push(ogawayama::stub::Metadata::ColumnType::Type::INT32, 4);
                result->get_binary_oarchive() << metadata;
                
                auto row_queue = std::make_unique<ogawayama::common::RowQueue>
                    (shared_memory_ptr->shm_name(ogawayama::common::param::resultset, id, request_message.get_ivalue()).c_str(),
                     managed_shared_memory_ptr);

                std::cerr << __func__ << " " << request_message.get_string() << " " << id << " " << request_message.get_ivalue() << std::endl;
                
                row_queue->put_next_column<std::int32_t> (1);
                row_queue->push_writing_row();
                row_queue->push_writing_row(); // End Of Row
            }
            break;
        case ogawayama::common::ChannelMessage::Type::NEXT:
            std::cerr << __func__ << " " << __LINE__ << std::endl;
            result->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK);
            break;
        case ogawayama::common::ChannelMessage::Type::DISCONNECT:
            std::cerr << __func__ << " " << __LINE__ << ": exiting" << std::endl;
            return;
        default:
            std::cerr << "recieved an illegal request_message" << std::endl;
            return;
        }
    }
}
    
}  // ogawayama::server
