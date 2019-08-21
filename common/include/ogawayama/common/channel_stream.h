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

#include <boost/circular_buffer.hpp>
#include <boost/bind.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>

namespace ogawayama::common {

const std::size_t BUFFER_SIZE = 4096; // 4K byte (tantative)

/**
 * @brief one to one communication channel, intended for communication between server and stub through boost binary_archive.
 */
class ChannelStream : public std::streambuf
{
public:
    /**
     * @brief core class in the communication channel, using boost::circular_buffer.
     */
    class BoundedBuffer {
        using AllocatorType = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;

    public:
        /**
         * @brief Construct a new object.
         */
        BoundedBuffer(AllocatorType allocator, std::size_t capacity = BUFFER_SIZE) : m_container_(capacity, allocator) {}
        /**
         * @brief Copy and move constructers are deleted.
         */
        BoundedBuffer(BoundedBuffer const&) = delete;
        BoundedBuffer(BoundedBuffer&&) = delete;
        BoundedBuffer& operator = (BoundedBuffer const&) = delete;
        BoundedBuffer& operator = (BoundedBuffer&&) = delete;

        /**
         * @brief push one char data into the circular_buffer.
         * @param item char data to be pushed.
         */
        void push(char item)
        {
            bool was_empty = !is_not_empty();
            
            if(!is_not_full()) {
                boost::interprocess::scoped_lock lock(m_full_mutex_);
                if(!is_not_full()) {
                    m_not_full_.wait(lock, boost::bind(&BoundedBuffer::is_not_full, this));
                }
                m_container_.push_back(item);
                lock.unlock();
            } else {
                m_container_.push_back(item);
            }
            ack = false;
            if(was_empty) {
                boost::interprocess::scoped_lock lock(m_empty_mutex_);
                lock.unlock();
                m_not_empty_.notify_one();
            }
        }
        
        /**
         * @brief pop one char data from the circular_buffer.
         * @param pItem points where poped char data to be stored.
         */
        void pop(char* pItem) {
            bool was_full = !is_not_full();
            
            if(!is_not_empty()) {
                boost::interprocess::scoped_lock lock(m_empty_mutex_);
                if(!is_not_empty()) {
                    m_not_empty_.wait(lock, boost::bind(&BoundedBuffer::is_not_empty, this));
                }
                *pItem = m_container_.front(); m_container_.pop_front();
                lock.unlock();
            } else {
                *pItem = m_container_.front(); m_container_.pop_front();
            }
            if(was_full) {
                boost::interprocess::scoped_lock lock(m_full_mutex_);
                lock.unlock();
                m_not_full_.notify_one();
            }
        }
        
        /**
         * @brief waiting acknowledge from the other side.
         */
        void recieve_ack() {
            boost::interprocess::scoped_lock lock(m_ack_mutex_);
            m_not_ack_.wait(lock, boost::bind(&BoundedBuffer::is_acked, this));
            lock.unlock();
        }

        /**
         * @brief send acknowledge to the other side.
         */
        void send_ack() {
            boost::interprocess::scoped_lock lock(m_ack_mutex_);
            ack = true;
            lock.unlock();
            m_not_ack_.notify_one();
        }
            
    private:
        bool is_not_empty() const { return (m_container_.array_one().second + m_container_.array_two().second) > 0; }
        bool is_not_full() const { return (m_container_.array_one().second + m_container_.array_two().second) < m_container_.capacity(); }
        bool is_acked() const { return ack; }

        boost::circular_buffer<char, AllocatorType> m_container_;
        boost::interprocess::interprocess_mutex m_empty_mutex_{};
        boost::interprocess::interprocess_mutex m_full_mutex_{};
        boost::interprocess::interprocess_mutex m_ack_mutex_{};
        boost::interprocess::interprocess_condition m_not_empty_{};
        boost::interprocess::interprocess_condition m_not_full_{};
        boost::interprocess::interprocess_condition m_not_ack_{};
        bool ack {false};
    };
    
public:
    public:
    /**
     * @brief Construct a new object.
     */
    ChannelStream(char const* name, boost::interprocess::managed_shared_memory *mem) : mem_(mem) {
        buffer_ = mem->find_or_construct<BoundedBuffer>(name)(mem->get_segment_manager());
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

private:
    BoundedBuffer *buffer_;
    boost::interprocess::managed_shared_memory *mem_;
};

};  // namespace ogawayama::common

#endif //  CHANNEL_STREAM_H_
