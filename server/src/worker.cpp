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

#include "shakujo/common/core/type/Int.h"
#include "shakujo/common/core/type/Float.h"
#include "shakujo/common/core/type/Char.h"

#include "worker.h"

namespace ogawayama::server {

Worker::Worker(umikongo::Database *db, ogawayama::common::SharedMemory *shm, std::size_t id) : db_(db), shared_memory_ptr_(shm), id_(id)
{
    auto managed_shared_memory_ptr = shared_memory_ptr_->get_managed_shared_memory_ptr();
    request_ = std::make_unique<ogawayama::common::ChannelStream>(shared_memory_ptr_->shm_name(ogawayama::common::param::request, id_).c_str(), managed_shared_memory_ptr);
    result_ = std::make_unique<ogawayama::common::ChannelStream>(shared_memory_ptr_->shm_name(ogawayama::common::param::result, id_).c_str(), managed_shared_memory_ptr);

    result_->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK);
}

void Worker::run()
{
    while(true) {
        ogawayama::common::ChannelMessage request_message;
        try {
            request_->get_binary_iarchive() >> request_message;
            std::cerr << __func__ << " " << __LINE__ << ": recieved normally" << std::endl;
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return;
        }
        
        switch (request_message.get_type()) {
        case ogawayama::common::ChannelMessage::Type::EXECUTE_STATEMENT:
            result_->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK, 1);

            std::cerr << request_message.get_string() << std::endl;
            
            break;
        case ogawayama::common::ChannelMessage::Type::EXECUTE_QUERY:
            execute_query(request_message.get_string(), request_message.get_ivalue());
            break;
        case ogawayama::common::ChannelMessage::Type::NEXT:
            std::cerr << __func__ << " " << __LINE__ << std::endl;
            result_->get_binary_oarchive() << ogawayama::common::ChannelMessage(ogawayama::common::ChannelMessage::Type::OK);
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

void Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    row_queues_.at(rid) = std::make_unique<ogawayama::common::RowQueue>
        (shared_memory_ptr_->shm_name(ogawayama::common::param::resultset, id_, rid).c_str(), shared_memory_ptr_->get_managed_shared_memory_ptr());
    metadatas_.at(rid).clear();
    
    auto executable = db_->compile(sql);
    auto metadata = executable->metadata();
    for (auto t: metadata->column_types()) {
        switch(t->kind()) {
        case shakujo::common::core::Type::Kind::INT:
            switch((static_cast<shakujo::common::core::type::Numeric const *>(t))->size()) {
            case 16:
                metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::INT16, 2);
                break;
            case 32:
                metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::INT32, 4);
                break;
            case 64:
                metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::INT64, 8);
                break;
            }
        case shakujo::common::core::Type::Kind::FLOAT:
            switch((static_cast<shakujo::common::core::type::Float const *>(t))->size()) {
            case 32:
                metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::FLOAT32, 4);
                break;
            case 64:
                metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::FLOAT64, 8);
                break;
            }
        case shakujo::common::core::Type::Kind::CHAR:
            metadatas_.at(rid).push(ogawayama::stub::Metadata::ColumnType::Type::TEXT,
                                    (static_cast<shakujo::common::core::type::Char const *>(t))->size());
            break;
        }
    }
    result_->get_binary_oarchive() << metadatas_.at(rid);

#if 0
    auto query = "SELECT * FROM T2";

    for(int i=0; i < 5; ++i) {
        // recycling executable query
        auto iterator = context->execute_query(executable.get());
        Closer result_closer{*iterator};
        for (auto row = iterator->next(); row != nullptr; row = iterator->next()) {
            std::vector<std::any> rec{};
            auto impl = dynamic_cast<RowImpl*>(row);
            res.emplace_back(impl->values());
        }
    }
    
} catch (Exception& e) {
        if (e.reason() == Exception::ReasonCode::ERR_UNSUPPORTED) {
            // function not supported, skip this test
            LOG(INFO) << "Function not supported, skipping test " << ::testing::UnitTest::GetInstance()->current_test_info()->name();
            return;
        }
        std::cerr << e.what() << std::endl;
        throw;
    }


    
    std::cerr << __func__ << " " << sql << " " << id_ << " " << rid << std::endl;
                
    row_queues_.at(rid)->put_next_column<std::int32_t> (1);
    row_queues_.at(rid)->push_writing_row();
    row_queues_.at(rid)->push_writing_row(); // End Of Row

#endif



}
    
}  // ogawayama::server
