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
#ifndef CHANNEL_STREAM_H_
#define CHANNEL_STREAM_H_

#include <atomic>

#include "boost/bind.hpp"
#include "boost/interprocess/managed_shared_memory.hpp"
#include "boost/interprocess/allocators/allocator.hpp"
#include "boost/interprocess/containers/string.hpp"
#include "boost/interprocess/sync/interprocess_condition.hpp"
#include "boost/interprocess/sync/interprocess_mutex.hpp"

#include "ogawayama/stub/error_code.h"
#include "ogawayama/common/shared_memory.h"

namespace ogawayama::common {

using VoidAllocator = boost::interprocess::allocator<void, boost::interprocess::managed_shared_memory::segment_manager>;
using CharAllocator = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;
using ShmString = boost::interprocess::basic_string<char, std::char_traits<char>, CharAllocator>;

class ChannelStream;
/**
 * @brief Messages exchanged via channel
 */
class CommandMessage
{
public:
    /**
     * @brief represents a type.
     */
    enum class Type {

        /**
         * @brief
         */
        OK,

        /**
         * @brief
         */
        CONNECT,

        /**
         * @brief
         */
        DISCONNECT,

        /**
         * @brief
         */
        EXECUTE_STATEMENT,

        /**
         * @brief
         */
        EXECUTE_QUERY,

        /**
         * @brief
         */
        NEXT,

        /**
         * @brief
         */
        COMMIT,

        /**
         * @brief
         */
        ROLLBACK,

        /**
         * @brief
         */
        TERMINATE,

        /**
         * @brief
         */
        DUMP_DATABASE,

        /**
         * @brief
         */
        LOAD_DATABASE,
    };

    CommandMessage() = delete;
    CommandMessage(VoidAllocator allocator) : string_(allocator) {}
    CommandMessage(const CommandMessage&) = delete;
    CommandMessage& operator=(const CommandMessage&) = delete;
    CommandMessage(CommandMessage&&) = delete;
    CommandMessage& operator=(CommandMessage&&) = delete;

    CommandMessage( Type type, std::size_t ivalue, std::string_view string ) : type_(type), ivalue_(ivalue), string_(string) {}
    CommandMessage( Type type, std::size_t ivalue ) : CommandMessage(type, ivalue, std::string()) {}
    CommandMessage( Type type ) : CommandMessage(type, 0, std::string()) {}

    Type get_type() const { return type_; }
    std::size_t get_ivalue() const { return ivalue_; }
    std::string_view get_string() const { return std::string_view(string_.data(), string_.size()); }

private:
    Type type_;
    std::size_t ivalue_;
    ShmString string_;

    friend class ChannelStream;
};

/**
 * @brief one to one communication channel, intended for communication between server and stub through boost binary_archive.
 */
class ChannelStream
{
public:
    /**
     * @brief core class in the communication channel, using boost::circular_buffer.
     */
    class MsgBuffer {
    public:
        /**
         * @brief Construct a new object.
         */
        MsgBuffer(VoidAllocator allocator) : message_(allocator) {}
        /**
         * @brief Copy and move constructers are deleted.
         */
        MsgBuffer(MsgBuffer const&) = delete;
        MsgBuffer(MsgBuffer&&) = delete;
        MsgBuffer& operator = (MsgBuffer const&) = delete;
        MsgBuffer& operator = (MsgBuffer&&) = delete;

        /**
         * @brief waiting acknowledge from the other side.
         */
        void wait() {
            boost::interprocess::scoped_lock lock(m_notify_mutex_);
            m_not_notify_.wait(lock, boost::bind(&MsgBuffer::is_notified, this));
            lock.unlock();
        }

        /**
         * @brief send acknowledge to the other side.
         */
        void notify() {
            boost::interprocess::scoped_lock lock(m_notify_mutex_);
            notified_ = true;
            lock.unlock();
            m_not_notify_.notify_one();
        }

        /**
         * @brief lock this channel. (for server channel)
         */
        void lock() {
            boost::interprocess::scoped_lock lock(m_lock_mutex_);
            m_not_locked_.wait(lock, boost::bind(&MsgBuffer::is_not_locked, this));
            locked_ = true;
            lock.unlock();
        }

        /**
         * @brief unlock this channel.
         */
        void unlock() {
            boost::interprocess::scoped_lock lock(m_lock_mutex_);
            locked_ = false;
            lock.unlock();
            m_not_locked_.notify_one();
        }

        void send_req(ogawayama::common::CommandMessage::Type type, std::size_t ivalue, std::string_view string) {
            boost::interprocess::scoped_lock lock(m_req_mutex_);
            m_req_invalid_.wait(lock, boost::bind(&MsgBuffer::is_req_invalid, this));
            {
                message_.type_ = type;
                message_.ivalue_ = ivalue;
                message_.string_ = string;
            }
            req_valid_ = true;
            lock.unlock();
            m_req_valid_.notify_one();
        }
        void send_ack(ogawayama::stub::ErrorCode err_code) {
            boost::interprocess::scoped_lock lock(m_ack_mutex_);
            m_ack_invalid_.wait(lock, boost::bind(&MsgBuffer::is_ack_invalid, this));
            {
                err_code_ = err_code;
            }
            ack_valid_ = true;
            lock.unlock();
            m_ack_valid_.notify_one();
        }
        void recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t & ivalue) {
            boost::interprocess::scoped_lock lock(m_req_mutex_);
            m_req_valid_.wait(lock, boost::bind(&MsgBuffer::is_req_valid, this));
            {
                type = message_.type_;
                ivalue = message_.ivalue_;
            }
            req_valid_ = false;
            lock.unlock();
            m_req_invalid_.notify_one();
        }
        void recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t & ivalue, std::string_view & string) {
            boost::interprocess::scoped_lock lock(m_req_mutex_);
            m_req_valid_.wait(lock, boost::bind(&MsgBuffer::is_req_valid, this));
            {
                type = message_.type_;
                ivalue = message_.ivalue_;
                string = message_.string_;
            }
            req_valid_ = false;
            lock.unlock();
            m_req_invalid_.notify_one();
        }
        void recv_ack(ogawayama::stub::ErrorCode &reply) {
            boost::interprocess::scoped_lock lock(m_ack_mutex_);
            m_ack_valid_.wait(lock, boost::bind(&MsgBuffer::is_ack_valid, this));
            {
                reply = err_code_;
            }
            ack_valid_ = false;
            lock.unlock();
            m_ack_invalid_.notify_one();
        }
        
    private:
        bool is_notified() const { return notified_; }
        bool is_not_locked() const { return !locked_; }
        bool is_req_valid() const { return req_valid_; }
        bool is_ack_valid() const { return ack_valid_; }
        bool is_req_invalid() const { return !req_valid_; }
        bool is_ack_invalid() const { return !ack_valid_; }

        CommandMessage message_;
        ogawayama::stub::ErrorCode err_code_;

        boost::interprocess::interprocess_mutex m_req_mutex_{};
        boost::interprocess::interprocess_mutex m_ack_mutex_{};
        boost::interprocess::interprocess_mutex m_notify_mutex_{};
        boost::interprocess::interprocess_mutex m_lock_mutex_{};
        boost::interprocess::interprocess_condition m_not_notify_{};
        boost::interprocess::interprocess_condition m_not_locked_{};
        boost::interprocess::interprocess_condition m_req_valid_{};
        boost::interprocess::interprocess_condition m_req_invalid_{};
        boost::interprocess::interprocess_condition m_ack_valid_{};
        boost::interprocess::interprocess_condition m_ack_invalid_{};
        bool req_valid_{false};
        bool ack_valid_{false};
        bool notified_{false};
        bool locked_{false};
    };
    
public:
    public:
    /**
     * @brief Construct a new object.
     */
    ChannelStream(char const* name, boost::interprocess::managed_shared_memory *mem, bool owner) : owner_(owner), mem_(mem)
    {
        if (owner_) {
            mem->destroy<MsgBuffer>(name);
            buffer_ = mem->construct<MsgBuffer>(name)(mem->get_segment_manager());
            strncpy(name_, name, param::MAX_NAME_LENGTH);
        } else {
            buffer_ = mem->find<MsgBuffer>(name).first;
            assert(buffer_);
        }
    }
    ChannelStream(char const* name, boost::interprocess::managed_shared_memory *mem) : ChannelStream(name, mem, false) {}

    /**
     * @brief Destruct this object.
     */
    ~ChannelStream()
    {
        if (owner_) {
            mem_->destroy<MsgBuffer>(name_);
        }
    }

    /**
     * @brief waiting acknowledge from the other side.
     */
    void wait() {
        buffer_->wait();
    }

    /**
     * @brief send acknowledge to the other side.
     */
    void notify() {
        buffer_->notify();
    }

    /**
     * @brief lock this channel. (for server channel)
     */
    void lock() {
        buffer_->lock();
    }

    /**
     * @brief unlock this channel.
     */
    void unlock() {
        buffer_->unlock();
    }

    void send_req(ogawayama::common::CommandMessage::Type type, std::size_t ivalue = 0, std::string_view string = "") {
        buffer_->send_req(type, ivalue, string);
    }
    void recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t &ivalue) {
        buffer_->recv_req(type, ivalue);
    }
    void recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t &ivalue, std::string_view &string) {
        buffer_->recv_req(type, ivalue, string);
    }
    void send_ack(ogawayama::stub::ErrorCode err_code) {
        buffer_->send_ack(err_code);
    }
    void recv_ack(ogawayama::stub::ErrorCode &reply) {
        buffer_->recv_ack(reply);
    }

private:
    MsgBuffer *buffer_;
    const bool owner_;
    boost::interprocess::managed_shared_memory *mem_;
    char name_[param::MAX_NAME_LENGTH];
};

};  // namespace ogawayama::common

#endif //  CHANNEL_STREAM_H_
