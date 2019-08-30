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

    result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::OK;
}

void Worker::run()
{
    while(true) {
        ogawayama::common::CommandMessage command_message;
        try {
            request_->get_binary_iarchive() >> command_message;
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return;
        }
        
        switch (command_message.get_type()) {
        case ogawayama::common::CommandMessage::Type::EXECUTE_STATEMENT:
            execute_statement(command_message.get_string());
            break;
        case ogawayama::common::CommandMessage::Type::EXECUTE_QUERY:
            execute_query(command_message.get_string(), command_message.get_ivalue());
            break;
        case ogawayama::common::CommandMessage::Type::NEXT:
            next(command_message.get_ivalue());
            result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::OK;
            break;
        case ogawayama::common::CommandMessage::Type::COMMIT:
            if (!transaction_) {
                result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::NO_TRANSACTION;
            }
            transaction_->commit();
            result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::OK;
            break;
        case ogawayama::common::CommandMessage::Type::ROLLBACK:
            if (!transaction_) {
                result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::NO_TRANSACTION;
            }
            //            transaction_->rollback();  FIXME
            result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::OK;
            break;
        case ogawayama::common::CommandMessage::Type::DISCONNECT:
            request_->notify();
            return;
        default:
            std::cerr << "recieved an illegal command message" << std::endl;
            return;
        }
    }
}

void Worker::execute_statement(std::string_view sql)
{
    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    try {
        context_->execute_statement(sql);
        result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::OK;
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            // function not supported, skip this test
            result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::UNSUPPORTED;
            return;
        }
        std::cerr << e.what() << std::endl;
        result_->get_binary_oarchive() << ogawayama::stub::ErrorCode::UNKNOWN;
    }
}

void Worker::execute_query(std::string_view sql, std::size_t rid)
{
    if (!transaction_) {
        transaction_ = db_->create_transaction();
        context_ = transaction_->context();
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }
    cursors_.at(rid).clear();
    cursors_.at(rid).row_queue_ = std::make_unique<ogawayama::common::RowQueue>
        (shared_memory_ptr_->shm_name(ogawayama::common::param::resultset, id_, rid).c_str(), shared_memory_ptr_->get_managed_shared_memory_ptr());
    
    auto executable = db_->compile(sql);
    auto metadata = executable->metadata();
    for (auto t: metadata->column_types()) {
        switch(t->kind()) {
        case shakujo::common::core::Type::Kind::INT:
            switch((static_cast<shakujo::common::core::type::Numeric const *>(t))->size()) {
            case 16:
                cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::INT16, 2);
                break;
            case 32:
                cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::INT32, 4);
                break;
            case 64:
                cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::INT64, 8);
                break;
            }
            break;
        case shakujo::common::core::Type::Kind::FLOAT:
            switch((static_cast<shakujo::common::core::type::Float const *>(t))->size()) {
            case 32:
                cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::FLOAT32, 4);
                break;
            case 64:
                cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::FLOAT64, 8);
                break;
            }
            break;
        case shakujo::common::core::Type::Kind::CHAR:
            cursors_.at(rid).metadata_.push(ogawayama::stub::Metadata::ColumnType::Type::TEXT,
                                    (static_cast<shakujo::common::core::type::Char const *>(t))->size());
            break;
        }
    }
    result_->get_binary_oarchive() << cursors_.at(rid).metadata_;
    
    cursors_.at(rid).iterator_ = context_->execute_query(executable.get());
}

void Worker::next(std::size_t rid)
{
    auto row = cursors_.at(rid).iterator_->next();
    if (row != nullptr) {
        for (auto t: cursors_.at(rid).metadata_.get_types()) {
            std::size_t cindex = cursors_.at(rid).row_queue_->get_cindex();
            if (row->is_null(cindex)) {
                cursors_.at(rid).row_queue_->put_next_column(std::monostate());
            } else {
                switch (t.get_type()) {
                case ogawayama::stub::Metadata::ColumnType::Type::INT16:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<std::int16_t>(cindex)); break;
                case ogawayama::stub::Metadata::ColumnType::Type::INT32:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<std::int32_t>(cindex)); break;
                case ogawayama::stub::Metadata::ColumnType::Type::INT64:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<std::int64_t>(cindex)); break;
                case ogawayama::stub::Metadata::ColumnType::Type::FLOAT32:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<float>(cindex)); break;
                case ogawayama::stub::Metadata::ColumnType::Type::FLOAT64:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<double>(cindex)); break;
                case ogawayama::stub::Metadata::ColumnType::Type::TEXT:
                    cursors_.at(rid).row_queue_->put_next_column(row->get<std::string_view>(cindex)); break;
                }
            }
        }
    }
    cursors_.at(rid).row_queue_->push_writing_row();
}

}  // ogawayama::server
