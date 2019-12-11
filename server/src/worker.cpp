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

#include "glog/logging.h"

#include "shakujo/common/core/type/Int.h"
#include "shakujo/common/core/type/Float.h"
#include "shakujo/common/core/type/Char.h"

#include "worker.h"

namespace ogawayama::server {

Worker::Worker(umikongo::Database *db, ogawayama::common::SharedMemory *shm, std::size_t id) : db_(db), shared_memory_ptr_(shm), id_(id)
{
    auto managed_shared_memory_ptr = shared_memory_ptr_->get_managed_shared_memory_ptr();
    channel_ = std::make_unique<ogawayama::common::ChannelStream>(shared_memory_ptr_->shm_name(ogawayama::common::param::channel, id_).c_str(), shared_memory_ptr_);
    channel_->send_ack(ERROR_CODE::OK);
}

void Worker::run()
{
    while(true) {
        ogawayama::common::CommandMessage::Type type;
        std::size_t ivalue;
        std::string_view string;
        try {
            if (channel_->recv_req(type, ivalue, string) != ERROR_CODE::OK) {
                std::cerr << __func__ << " " << __LINE__ << ": exiting" << std::endl;
                return;
            }
            VLOG(1) << __func__ << ":" << __LINE__ << " recieved " << ivalue << " " << ogawayama::common::type_name(type) << " \"" << string << "\"";
        } catch (std::exception &ex) {
            std::cerr << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
            return;
        }

        switch (type) {
        case ogawayama::common::CommandMessage::Type::EXECUTE_STATEMENT:
            execute_statement(string);
            break;
        case ogawayama::common::CommandMessage::Type::EXECUTE_QUERY:
            execute_query(string, ivalue);
            next(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::NEXT:
            channel_->send_ack(ERROR_CODE::OK);
            next(ivalue);
            break;
        case ogawayama::common::CommandMessage::Type::COMMIT:
            if (!transaction_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
                break;
            }
            transaction_->commit();
            channel_->send_ack(ERROR_CODE::OK);
            clear();
            break;
        case ogawayama::common::CommandMessage::Type::ROLLBACK:
            if (!transaction_) {
                channel_->send_ack(ERROR_CODE::NO_TRANSACTION);
            }
            transaction_->abort();
            channel_->send_ack(ERROR_CODE::OK);
            clear();
            break;
        case ogawayama::common::CommandMessage::Type::DISCONNECT:
            if (transaction_) {
                transaction_->abort();
            }
            clear();
            channel_->bye_and_notify();
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
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
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
    cursors_.at(rid).row_queue_ = std::make_unique<ogawayama::common::RowQueue>
        (shared_memory_ptr_->shm_name(ogawayama::common::param::resultset, id_, rid).c_str(), shared_memory_ptr_);
    
    try {
        cursors_.at(rid).executable_ = db_->compile(sql);
        auto metadata = cursors_.at(rid).executable_->metadata();
        for (auto& t: metadata->column_types()) {
            switch(t->kind()) {
            case shakujo::common::core::Type::Kind::INT:
                switch((static_cast<shakujo::common::core::type::Numeric const *>(t))->size()) {
                case 16:
                    cursors_.at(rid).row_queue_->push_type(TYPE::INT16, 2);
                    break;
                case 32:
                    cursors_.at(rid).row_queue_->push_type(TYPE::INT32, 4);
                    break;
                case 64:
                    cursors_.at(rid).row_queue_->push_type(TYPE::INT64, 8);
                    break;
                }
                break;
            case shakujo::common::core::Type::Kind::FLOAT:
                switch((static_cast<shakujo::common::core::type::Float const *>(t))->size()) {
                case 32:
                    cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT32, 4);
                    break;
                case 64:
                    cursors_.at(rid).row_queue_->push_type(TYPE::FLOAT64, 8);
                    break;
                }
                break;
            case shakujo::common::core::Type::Kind::CHAR:
                cursors_.at(rid).row_queue_->push_type(TYPE::TEXT,
                                                       (static_cast<shakujo::common::core::type::Char const *>(t))->size());
                break;
            case shakujo::common::core::Type::Kind::STRING:
                cursors_.at(rid).row_queue_->push_type(TYPE::TEXT, INT32_MAX);
                break;
            default:
                std::cerr << "unsurpported data type: " << t->kind() << std::endl;
                break;
            }
        }
    
        cursors_.at(rid).iterator_ = context_->execute_query(cursors_.at(rid).executable_.get());
        channel_->send_ack(ERROR_CODE::OK);
    } catch (umikongo::Exception& e) {
        if (e.reason() == umikongo::Exception::ReasonCode::ERR_UNSUPPORTED) {
            channel_->send_ack(ERROR_CODE::UNSUPPORTED);
            return;
        }
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

void Worker::next(std::size_t rid)
{
    try {
        auto& rq = cursors_.at(rid).row_queue_;
        std::size_t limit = rq->get_requested();
        for(std::size_t i = 0; i < limit; i++) {
            auto row = cursors_.at(rid).iterator_->next();
            if (row != nullptr) {
                for (auto& t: rq->get_metadata_ptr()->get_types()) {
                    std::size_t cindex = rq->get_cindex();
                    if (row->is_null(cindex)) {
                        rq->put_next_column(std::monostate());
                    } else {
                        switch (t.get_type()) {
                        case TYPE::INT16:
                            rq->put_next_column(row->get<std::int16_t>(cindex)); break;
                        case TYPE::INT32:
                            rq->put_next_column(row->get<std::int32_t>(cindex)); break;
                        case TYPE::INT64:
                            rq->put_next_column(row->get<std::int64_t>(cindex)); break;
                        case TYPE::FLOAT32:
                            rq->put_next_column(row->get<float>(cindex)); break;
                        case TYPE::FLOAT64:
                            rq->put_next_column(row->get<double>(cindex)); break;
                        case TYPE::TEXT:
                            rq->put_next_column(row->get<std::string_view>(cindex)); break;
                        case TYPE::NULL_VALUE:
                            std::cerr << "NULL_VALUE type should not be used" << std::endl; break;
                        }
                    }
                }
                rq->push_writing_row();
            } else {
                rq->push_writing_row();
                break;
            }
        }
    } catch (umikongo::Exception& e) {
        std::cerr << e.what() << std::endl;
        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}

void Worker::clear()
{
    cursors_.clear();
    transaction_ = nullptr;
}

}  // ogawayama::server
