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

#include <atomic>

#include <boost/bind.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

#include <ogawayama/stub/error_code.h>
#include <ogawayama/common/shared_memory.h>

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
        PREPARE,

        /**
         * @brief
         */
        EXECUTE_PREPARED_STATEMENT,

        /**
         * @brief
         */
        EXECUTE_PREPARED_QUERY,

        /**
         * @brief
         */
        EXECUTE_CREATE_TABLE,

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
        CREATE_TABLE,

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
            while (true) {
                if (m_not_notify_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_notified, this))) {
                    break;
                }
            }
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
            while (true) {
                if (m_not_locked_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_not_locked, this))) {
                    break;
                }
            }
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
            while (true) {
                if (m_req_invalid_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_req_invalid, this))) {
                    break;
                }
            }
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
            while (true) {
                if (m_ack_invalid_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_ack_invalid, this))) {
                    break;
                }
            }
            {
                err_code_ = err_code;
            }
            ack_valid_ = true;
            lock.unlock();
            m_ack_valid_.notify_one();
        }
        ogawayama::stub::ErrorCode recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t & ivalue) {
            boost::interprocess::scoped_lock lock(m_req_mutex_);
            while (true) {
                if (m_req_valid_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_req_valid, this))) {
                    break;
                }
                return ogawayama::stub::ErrorCode::TIMEOUT;
            }
            {
                type = message_.type_;
                ivalue = message_.ivalue_;
            }
            req_valid_ = false;
            lock.unlock();
            m_req_invalid_.notify_one();
            return ogawayama::stub::ErrorCode::OK;
        }
        ogawayama::stub::ErrorCode recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t & ivalue, std::string_view & string) {
            boost::interprocess::scoped_lock lock(m_req_mutex_);
            while (true) {
                if (m_req_valid_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_req_valid, this))) {
                    break;
                }
                return ogawayama::stub::ErrorCode::TIMEOUT;
            }
            {
                type = message_.type_;
                ivalue = message_.ivalue_;
                string = message_.string_;
            }
            req_valid_ = false;
            lock.unlock();
            m_req_invalid_.notify_one();
            return ogawayama::stub::ErrorCode::OK;
        }
        ogawayama::stub::ErrorCode recv_ack() {
            boost::interprocess::scoped_lock lock(m_ack_mutex_);
            while (true) {
                if (m_ack_valid_.timed_wait(lock, timeout(), boost::bind(&MsgBuffer::is_ack_valid, this))) {
                    break;
                }
                return ogawayama::stub::ErrorCode::TIMEOUT;
            }
            ack_valid_ = false;
            lock.unlock();
            m_ack_invalid_.notify_one();
            return err_code_;
        }
        
        void hello() {
            is_partner_ = true;
        }
        void bye() {
            is_partner_ = false;
        }
        bool is_partner() {
            return is_partner_;
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

        boost::system_time timeout() { return boost::get_system_time() + boost::posix_time::milliseconds(param::TIMEOUT); }

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

        bool is_partner_{false};
    };
    
    /**
     * @brief Construct a new object.
     */
    ChannelStream(std::string_view name, SharedMemory *shared_memory, bool owner, bool always_connected = true) :
    shared_memory_(shared_memory), owner_(owner), always_connected_(always_connected)
    {
        strncpy(name_, name.data(), name.length());
        auto mem = shared_memory_->get_managed_shared_memory_ptr();
        if (owner_) {
            mem->destroy<MsgBuffer>(name_);
            buffer_ = mem->construct<MsgBuffer>(name_)(mem->get_segment_manager());
        } else {
            buffer_ = mem->find<MsgBuffer>(name_).first;
            if (buffer_ == nullptr) {
                throw SharedMemoryException("can't find shared memory for ChannelStream");
            }
            buffer_->hello();
        }
    }
    ChannelStream(std::string_view name, SharedMemory *shared_memory) : ChannelStream(name, shared_memory, false) {}

    /**
     * @brief Destruct this object.
     */
    ~ChannelStream()
    {
        if (owner_) {
            shared_memory_->get_managed_shared_memory_ptr()->destroy<MsgBuffer>(name_);
        } else {
            if (buffer_ != nullptr) {
                if (shared_memory_->get_managed_shared_memory_ptr()->find<MsgBuffer>(name_).first != nullptr) {
                    buffer_->bye();
                }
            }
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
    void send_req(ogawayama::common::CommandMessage::Type type, std::string_view string) {
        buffer_->send_req(type, 0, string);
    }
    ogawayama::stub::ErrorCode recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t &ivalue) {
        while (true) {
            auto retv = buffer_->recv_req(type, ivalue);
            if (retv != ogawayama::stub::ErrorCode::TIMEOUT) {
                return retv;
            }
            if (!is_alive()) {
                return ogawayama::stub::ErrorCode::SERVER_FAILURE;
            }
        }
    }
    ogawayama::stub::ErrorCode recv_req(ogawayama::common::CommandMessage::Type &type, std::size_t &ivalue, std::string_view &string) {
        while (true) {
            auto retv =  buffer_->recv_req(type, ivalue, string);
            if (retv != ogawayama::stub::ErrorCode::TIMEOUT) {
                return retv;
            }
            if (!is_alive()) {
                return ogawayama::stub::ErrorCode::SERVER_FAILURE;
            }
        }
    }
    void send_ack(ogawayama::stub::ErrorCode err_code) {
        buffer_->send_ack(err_code);
    }
    ogawayama::stub::ErrorCode recv_ack() {
        while (true) {
            auto retv = buffer_->recv_ack();
            if (retv != ogawayama::stub::ErrorCode::TIMEOUT) {
                return retv;
            }
            if (!is_alive()) {
                return ogawayama::stub::ErrorCode::SERVER_FAILURE;
            }
        }
    }
    void bye_and_notify() {
        buffer_->bye();
        buffer_->notify();
        buffer_ = nullptr;
    }
    void bye() {
        buffer_ = nullptr;
    }
    bool is_alive() {
        if (!shared_memory_->is_alive()) {
            return false;
        }
        if (owner_) {
            if (always_connected_) {
                return buffer_->is_partner();
            }
            return true;
        }
        try {
            return shared_memory_->get_managed_shared_memory_ptr()->find<MsgBuffer>(name_).first != nullptr;
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            return false;
        }
    }

private:
    SharedMemory *shared_memory_;
    MsgBuffer *buffer_;
    const bool owner_;
    const bool always_connected_;
    char name_[param::MAX_NAME_LENGTH]{};
};

constexpr std::string_view type_name(CommandMessage::Type type) {
    switch (type) {
    case CommandMessage::Type::OK: return "OK";
    case CommandMessage::Type::CONNECT: return "CONNECT";
    case CommandMessage::Type::DISCONNECT: return "DISCONNECT";
    case CommandMessage::Type::EXECUTE_STATEMENT: return "EXECUTE_STATEMENT";
    case CommandMessage::Type::EXECUTE_QUERY: return "EXECUTE_QUERY";
    case CommandMessage::Type::NEXT: return "NEXT";
    case CommandMessage::Type::COMMIT: return "COMMIT";
    case CommandMessage::Type::ROLLBACK: return "ROLLBACK";
    case CommandMessage::Type::CREATE_TABLE: return "CREATE_TABLE";
    case CommandMessage::Type::PREPARE: return "PREPARE";
    case CommandMessage::Type::EXECUTE_PREPARED_STATEMENT: return "EXECUTE_PREPARED_STATEMENT";
    case CommandMessage::Type::EXECUTE_PREPARED_QUERY: return "EXECUTE_PREPARED_QUERY";
    case CommandMessage::Type::EXECUTE_CREATE_TABLE: return "EXECUTE_CREATE_TABLE(v1 only)";
    case CommandMessage::Type::TERMINATE: return "TERMINATE";
    case CommandMessage::Type::DUMP_DATABASE: return "DUMP_DATABASE";
    case CommandMessage::Type::LOAD_DATABASE: return "LOAD_DATABASE";
    default: return "UNDEFINED";
    }
}

};  // namespace ogawayama::common
