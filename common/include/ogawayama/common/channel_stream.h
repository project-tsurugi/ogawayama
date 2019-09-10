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
#include "boost/interprocess/sync/interprocess_condition.hpp"
#include "boost/interprocess/sync/interprocess_mutex.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/binary_iarchive.hpp"

#include "ogawayama/stub/error_code.h"
#include "ogawayama/common/shared_memory.h"

namespace ogawayama::common {

/**
 * @brief one to one communication channel, intended for communication between server and stub through boost binary_archive.
 */
class ChannelStream : public std::streambuf
{
public:
    /**
     * @brief core class in the communication channel, using boost::circular_buffer.
     */
    class RingBuffer {
        using AllocatorType = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;

    public:
        /**
         * @brief Construct a new object.
         */
        RingBuffer() : pushed_(0), poped_(0) {}
        /**
         * @brief Copy and move constructers are deleted.
         */
        RingBuffer(RingBuffer const&) = delete;
        RingBuffer(RingBuffer&&) = delete;
        RingBuffer& operator = (RingBuffer const&) = delete;
        RingBuffer& operator = (RingBuffer&&) = delete;

        /**
         * @brief push one char data into the circular_buffer.
         * @param item char data to be pushed.
         */
        void push(char item)
        {
            if(!is_not_full()) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                m_not_full_.wait(lock, boost::bind(&RingBuffer::is_not_full, this));
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            m_container_[index(pushed_)] = item;
            std::atomic_thread_fence(std::memory_order_release);
            pushed_++;
            if (was_empty()) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                if (was_empty()) { m_not_empty_.notify_one(); }
            }
        }
        
        /**
         * @brief pop one char data from the circular_buffer.
         * @param pItem points where poped char data to be stored.
         */
        void pop(char* pItem) {
            if(!is_not_empty()) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                m_not_empty_.wait(lock, boost::bind(&RingBuffer::is_not_empty, this));
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            *pItem = m_container_[index(poped_)];
            std::atomic_thread_fence(std::memory_order_release);
            poped_++;
            if (was_full()) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                if (was_full()) { m_not_full_.notify_one(); }
            }
        }
        
        /**
         * @brief waiting acknowledge from the other side.
         */
        void wait() {
            boost::interprocess::scoped_lock lock(m_notify_mutex_);
            m_not_notify_.wait(lock, boost::bind(&RingBuffer::is_notified, this));
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
            m_not_locked_.wait(lock, boost::bind(&RingBuffer::is_not_locked, this));
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

    private:
        std::size_t index(std::size_t i) { return i % param::BUFFER_SIZE; }
        
        bool is_not_empty() const { return (pushed_ - poped_) > 0; }
        bool was_empty() const { return (pushed_ - poped_) == 1; }
        bool is_not_full() const { return (pushed_ - poped_) < param::BUFFER_SIZE; }
        bool was_full() const { return (pushed_ - poped_) == (param::BUFFER_SIZE - 1); }
        bool is_notified() const { return notified_; }
        bool is_not_locked() const { return !locked_; }
        std::size_t pushed_{0};
        std::size_t poped_{0};

        char m_container_[param::BUFFER_SIZE];
        boost::interprocess::interprocess_mutex m_mutex_{};
        boost::interprocess::interprocess_mutex m_notify_mutex_{};
        boost::interprocess::interprocess_mutex m_lock_mutex_{};
        boost::interprocess::interprocess_condition m_not_empty_{};
        boost::interprocess::interprocess_condition m_not_full_{};
        boost::interprocess::interprocess_condition m_not_notify_{};
        boost::interprocess::interprocess_condition m_not_locked_{};
        bool notified_ {false};
        bool locked_ {false};
    };
    
public:
    public:
    /**
     * @brief Construct a new object.
     */
    ChannelStream(char const* name, boost::interprocess::managed_shared_memory *mem, bool owner) : owner_(owner), mem_(mem)
    {
        if (owner_) {
            buffer_ = mem->construct<RingBuffer>(name)();
            strncpy(name_, name, param::MAX_NAME_LENGTH);
        } else {
            buffer_ = mem->find<RingBuffer>(name).first;
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
            mem_->destroy<RingBuffer>(name_);
        }
    }

    /**
     * @brief Implements methods inherited from std::streambuf.
     */
    virtual int overflow( int p_iChar = EOF )
    {
        buffer_->push(static_cast<char>(p_iChar));
        return true;
    }

    /**
     * @brief Implements methods inherited from std::streambuf.
     */
    virtual int uflow()
    {
        char rv;
        
        buffer_->pop(&rv);
        return static_cast<int>(rv);
    }

    /**
     * @brief get binary_iarchive associated with this object
     * @return a binary_iarchive associated with this object
     */
    boost::archive::binary_iarchive get_binary_iarchive()
    {
        return boost::archive::binary_iarchive(*this);
    }

    /**
     * @brief get binary_oarchive associated with this object
     * @return a binary_oarchive associated with this object
     */
    boost::archive::binary_oarchive get_binary_oarchive()
    {
        return boost::archive::binary_oarchive(*this);
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

private:
    RingBuffer *buffer_;
    const bool owner_;
    boost::interprocess::managed_shared_memory *mem_;
    char name_[param::MAX_NAME_LENGTH];
};

/**
 * @brief Messages exchanged via channel
 */
struct CommandMessage
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
    };

    CommandMessage() = default;
    CommandMessage(const CommandMessage&) = default;
    CommandMessage& operator=(const CommandMessage&) = default;
    CommandMessage(CommandMessage&&) = default;
    CommandMessage& operator=(CommandMessage&&) = default;

    CommandMessage( Type type, std::size_t ivalue, std::string_view string ) : type_(type), ivalue_(ivalue), string_(string) {}
    CommandMessage( Type type, std::size_t ivalue ) : CommandMessage(type, ivalue, std::string()) {}
    CommandMessage( Type type ) : CommandMessage(type, 0, std::string()) {}
    
    Type get_type() const { return type_; }
    std::size_t get_ivalue() const { return ivalue_; }
    std::string_view get_string() const { return std::string_view(string_.data(), string_.size()); }

private:
    Type type_;
    std::size_t ivalue_;
    std::string string_;

    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, unsigned int /*version*/)
    {
        ar & boost::serialization::make_nvp("type", type_);
        ar & boost::serialization::make_nvp("ivalue", ivalue_);
        ar & boost::serialization::make_nvp("string", string_);
    }    
};

};  // namespace ogawayama::common

namespace boost::serialization {

/**
 * @brief Method that does serialization dedicated for ogawayama::stub::ErrorCode
 * @param ar archiver
 * @param d variable in Channel_MessageType
 * @param file_version not used
 */
template<class Archive>
inline void serialize(Archive & ar,
                      ogawayama::stub::ErrorCode & d,
                      const unsigned int file_version)
{
    ar & d;
}

};  // namespace boost::serialization

#endif //  CHANNEL_STREAM_H_
