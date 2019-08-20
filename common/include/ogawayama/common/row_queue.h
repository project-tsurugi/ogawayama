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
#ifndef ROW_QUEUE_H_
#define ROW_QUEUE_H_

#include <variant>
#include <string_view>
#include "boost/interprocess/managed_shared_memory.hpp"
#include "boost/interprocess/allocators/allocator.hpp"
#include "boost/interprocess/containers/string.hpp"
#include "boost/interprocess/containers/vector.hpp"
#include "boost/lockfree/spsc_queue.hpp"
#include "boost/interprocess/sync/interprocess_condition.hpp"
#include "boost/interprocess/sync/interprocess_mutex.hpp"
#include "boost/bind.hpp"
#include "boost/interprocess/smart_ptr/shared_ptr.hpp"

namespace ogawayama::common {
    const std::size_t QUEUE_SIZE = 32; // QUEUE_SIZE must be greater than or equal to two. (32 is tantative)
    
    using VoidAllocator = boost::interprocess::allocator<void, boost::interprocess::managed_shared_memory::segment_manager>;
    using CharAllocator = boost::interprocess::allocator<char, boost::interprocess::managed_shared_memory::segment_manager>;
    using ShmString = boost::interprocess::basic_string<char, std::char_traits<char>, CharAllocator>;
    
    using ShmColumn = std::variant<std::monostate, std::int16_t, std::int32_t, std::int64_t, float, double, ShmString>;
    using ShmColumnAllocator = boost::interprocess::allocator<ShmColumn, boost::interprocess::managed_shared_memory::segment_manager>;
    
    using ShmRow = boost::interprocess::vector<ShmColumn,ShmColumnAllocator>;
    using ShmRowAllocator = boost::interprocess::allocator<ShmRow, boost::interprocess::managed_shared_memory::segment_manager>;
    using ShmQueue = boost::interprocess::vector<ShmRow, ShmRowAllocator>;

    using ShMemRef = boost::shared_ptr<boost::interprocess::managed_shared_memory>;
    
    /**
     * @brief one to one communication channel, intended for communication between server and stub through boost binary_archive.
     */
    class RowQueue
    {
    public:
        /**
         * @brief core class in the communication channel, using boost::circular_buffer.
         */
        class SpscQueue {
        public:
            /**
             * @brief Construct a new object.
             */
            SpscQueue(VoidAllocator allocator, std::size_t capacity = QUEUE_SIZE) : m_container_(allocator) {
                m_container_.resize(capacity, ShmRow(allocator));
            }
            /**
             * @brief Copy and move constructers are deleted.
             */
            SpscQueue(SpscQueue const&) = delete;
            SpscQueue(SpscQueue&&) = delete;
            SpscQueue& operator = (SpscQueue const&) = delete;
            SpscQueue& operator = (SpscQueue&&) = delete;
            
            /**
             * @brief push one row into the queue and clear the next row (vector).
             * @param item char data to be pushed.
             */
            void push()
            {
                bool was_empty = !is_not_empty();
                
                if(!is_not_full()) {
                    boost::interprocess::scoped_lock lock(m_full_mutex_);
                    if(!is_not_full()) {
                        m_not_full_.wait(lock, boost::bind(&SpscQueue::is_not_full, this));
                    }
                    lock.unlock();
                }
                if(was_empty) {
                    boost::interprocess::scoped_lock lock(m_empty_mutex_);
                    lock.unlock();
                    m_not_empty_.notify_one();
                }
                pushed_++;
                get_row().clear();
            }
            
            /**
             * @brief pop one row from the queue, never invoke more than once.
             * @return reference to the row at the front of the queue
             */
            ShmRow & pop() {
                bool was_full = !is_not_full();
                
                if(!is_not_empty()) {
                    boost::interprocess::scoped_lock lock(m_empty_mutex_);
                    if(!is_not_empty()) {
                        m_not_empty_.wait(lock, boost::bind(&SpscQueue::is_not_empty, this));
                    }
                    lock.unlock();
                }
                if(was_full) {
                    boost::interprocess::scoped_lock lock(m_full_mutex_);
                    lock.unlock();
                    m_not_full_.notify_one();
                }
                return m_container_.at(index(poped_++));
            }
            
            /**
             * @brief get the row to which column data is to be stored.
             * @return reference of the row at the back of the queue
             */
            ShmRow & get_row() { return m_container_.at(index(pushed_)); }
            
        private:
            bool is_not_empty() const { return pushed_ > poped_; }
            bool is_not_full() const { return (pushed_ - poped_) < (QUEUE_SIZE - 1); }
            std::size_t index(std::size_t n) { return n %  QUEUE_SIZE; }
            
            ShmQueue m_container_;
            std::size_t pushed_{0};
            std::size_t poped_{0};

            boost::interprocess::interprocess_mutex m_empty_mutex_{};
            boost::interprocess::interprocess_mutex m_full_mutex_{};
            boost::interprocess::interprocess_condition m_not_empty_{};
            boost::interprocess::interprocess_condition m_not_full_{};
        };
        
    public:
        /**
         * @brief Construct a new object.
         */
        RowQueue(char const* name, boost::interprocess::managed_shared_memory *mem) : mem_(mem) {
            queue_ = boost::interprocess::make_managed_shared_ptr<SpscQueue, boost::interprocess::managed_shared_memory>(mem_->find_or_construct<SpscQueue>(name)(mem_->get_segment_manager()), *mem_);
            assert(queue_);
        }
        /**
         * @brief put a column value (of other than string) to the row at the back of the queue.
         */
        template<typename T>
            void put_next_column(T v) {
            ShmColumn column = v;
            queue_->get_row().emplace_back(column);
        }
        /**
         * @brief put a string column value to the row at the back of the queue.
         */
        void put_next_column(std::string_view v) {
            ShmString column_string(mem_->get_segment_manager());
            column_string.assign(v.begin(), v.end());
            ShmColumn column = column_string;
            queue_->get_row().emplace_back(column);
        }
        /**
         * @brief push the row into the queue.
         */
        void push() {
            queue_->push();
        }
        /**
         * @brief get the queue pointer to retrieve the row, for reference count maintenance.
         * @return shared_ptr to the queue
         */
        auto get_queue() {
            return queue_;
        }
        /**
         * @brief get the reference count to the queue to see if anyone uses the queue.
         * @return the reference count to the queue
         */
        auto use_count() {
            return queue_.use_count();
        }
        
    private:
        boost::interprocess::managed_shared_ptr<SpscQueue, boost::interprocess::managed_shared_memory>::type queue_;
        boost::interprocess::managed_shared_memory *mem_;
    };
    
};  // namespace ogawayama::common

#endif //  ROW_QUEUE_H_
