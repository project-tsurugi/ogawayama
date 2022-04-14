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

#include <boost/lockfree/spsc_queue.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/bind.hpp>
#include <boost/interprocess/smart_ptr/shared_ptr.hpp>

#include <ogawayama/stub/metadata.h>
#include <ogawayama/common/shared_memory.h>

namespace ogawayama::common {

    using VoidAllocator = boost::interprocess::allocator<void, boost::interprocess::managed_shared_memory::segment_manager>;
    using ShMemRef = boost::shared_ptr<boost::interprocess::managed_shared_memory>;
    using ShmQueue = std::vector<ShmRowArgs, ShmRowArgsAllocator>;

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
            SpscQueue(VoidAllocator allocator, std::size_t capacity = param::QUEUE_SIZE)
                : m_container_(allocator), m_types_(allocator), allocator_(allocator), capacity_(capacity) {
                m_container_.resize(capacity, ShmRowArgs(allocator));
            }
            /**
             * @brief Copy and move constructers are deleted.
             */
            SpscQueue(SpscQueue const&) = delete;
            SpscQueue(SpscQueue&&) = delete;
            SpscQueue& operator = (SpscQueue const&) = delete;
            SpscQueue& operator = (SpscQueue&&) = delete;
            
            /**
             * @brief push the writing row into the queue.
             */
            ogawayama::stub::ErrorCode push()
            {
                if(!is_not_full()) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    while (true) {
                        if (m_not_full_.timed_wait(lock, timeout(), boost::bind(&SpscQueue::is_not_full, this))) {
                            break;
                        }
                        return ogawayama::stub::ErrorCode::TIMEOUT;
                    }
                }
                std::atomic_thread_fence(std::memory_order_acquire);
                pushed_++;
                if (was_empty()) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    if (was_empty()) { m_not_empty_.notify_one(); }
                }
                return ogawayama::stub::ErrorCode::OK;
            }
            
            /**
             * @brief pop the current row.
             */
            ogawayama::stub::ErrorCode pop() {
                if(!is_not_empty()) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    while (true) {
                        if (m_not_empty_.timed_wait(lock, timeout(), boost::bind(&SpscQueue::is_not_empty, this))) {
                            break;
                        }
                        return ogawayama::stub::ErrorCode::TIMEOUT;
                    }
                }
                std::atomic_thread_fence(std::memory_order_acquire);
                poped_++;
                if (was_full()) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    if (was_full()) { m_not_full_.notify_one(); }
                }
                return ogawayama::stub::ErrorCode::OK;
            }
            
            /**
             * @brief get the current row at the front of the queue
             * @return reference of the row at the front of the queue
             */
            ShmRowArgs & get_current_row() {
                return m_container_.at(index(poped_-1));
            }

            /**
             * @brief get the row to which column data is to be stored.
             * @return reference of the row at the back of the queue
             */
            ShmRowArgs & get_writing_row() {
                if(!is_not_full()) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    if(!is_not_full()) {
                        while (true) {
                            if (m_not_full_.timed_wait(lock, timeout(), boost::bind(&SpscQueue::is_not_full, this))) {
                                break;
                            }
                            throw SharedMemoryException("notify of timeout");
                        }
                    }
                    lock.unlock();
                }
                return m_container_.at(index(pushed_));
            }

            void resize_writing_row(std::size_t n) {
                get_writing_row().resize(n);
            }

            void clear()
            {
                m_container_.clear();
                m_container_.resize(capacity_, ShmRowArgs(allocator_));
                pushed_ = poped_ = 0;
                m_types_.clear();
                eor_ = closed_ = false;
            }

            void push_type(ogawayama::stub::Metadata::ColumnType::Type type, std::size_t length) {
                m_types_.push(type, length);
            }

            ogawayama::stub::Metadata const * get_metadata_ptr() {
                return &m_types_;
            }

            void hello() { is_partner_ = true; }
            void bye() { is_partner_ = false; }
            bool is_partner() const { return is_partner_; }

            void set_requested(std::size_t requested) { requested_ = requested; }
            std::size_t get_requested() const { return requested_; }
            std::size_t get_capacity() const { return capacity_; }

            void set_eor() { eor_ = true; }
            bool is_eor() const { return eor_; }
            void set_closed() { closed_ = true; }
            bool is_closed() const { return closed_; }

        private:
            bool is_not_empty() const { return (pushed_ - poped_) > 0; }
            bool was_empty() const { return (pushed_ - poped_) == 1; }
            bool is_not_full() const { return (pushed_ - poped_) < capacity_; }
            bool was_full() const { return (pushed_ - poped_) == capacity_; }
            std::size_t index(std::size_t n) const { return n %  capacity_; }
            
            ShmQueue m_container_;
            ogawayama::stub::Metadata m_types_;
            VoidAllocator allocator_;
            std::atomic_size_t capacity_;
            std::atomic_size_t pushed_{0};
            std::atomic_size_t poped_{0};
            std::atomic_size_t requested_{1};

            boost::system_time timeout() { return boost::get_system_time() + boost::posix_time::milliseconds(param::TIMEOUT); }

            boost::interprocess::interprocess_mutex m_mutex_{};
            boost::interprocess::interprocess_condition m_not_empty_{};
            boost::interprocess::interprocess_condition m_not_full_{};

            std::atomic_bool is_partner_{false};
            std::atomic_bool eor_{false};
            std::atomic_bool closed_{false};
        };
        
    public:
        /**
         * @brief Construct a new object.
         */
        RowQueue(std::string_view name, SharedMemory *shared_memory, bool owner) : shared_memory_(shared_memory), owner_(owner), cindex_(0)
        {
            strncpy(name_, name.data(), name.length());
            auto mem = shared_memory_->get_managed_shared_memory_ptr();
            if (owner_) {
                mem->destroy<SpscQueue>(name_);
                queue_ = mem->construct<SpscQueue>(name_)(mem->get_segment_manager());
            } else {
                queue_ = mem->find<SpscQueue>(name_).first;
                if (queue_ == nullptr) {
                    throw SharedMemoryException(std::string("can't find shared memory for RowQueue: ") + std::string(name));
                }
                queue_->hello();
            }
        }
        RowQueue(std::string_view name, SharedMemory *shared_memory) : RowQueue(name, shared_memory, false) {}

        /**
         * @brief Destruct this object.
         */
        ~RowQueue()
        {
            if (owner_) {
                shared_memory_->get_managed_shared_memory_ptr()->destroy<SpscQueue>(name_);
            } else {
                queue_->bye();
            }
        }

        /**
         * @brief put a column value (of other than string) to the row at the back of the queue.
         */
        template<typename T>
        ogawayama::stub::ErrorCode put_next_column(T v) {
            try {
                auto& row = queue_->get_writing_row();
                if (row.size() > cindex_) {
                    row.at(cindex_) = v;
                } else {
                    row.emplace_back(v);
                }
            }
            catch(const SharedMemoryException& ex) {
                return ogawayama::stub::ErrorCode::TIMEOUT;
            }
            cindex_++;
            return ogawayama::stub::ErrorCode::OK;
        }
        /**
         * @brief put a string column value to the row at the back of the queue.
         */
        ogawayama::stub::ErrorCode put_next_column(std::string_view v) {
            ShmString column_string(shared_memory_->get_managed_shared_memory_ptr()->get_segment_manager());
            column_string.assign(v.begin(), v.end());
            try {
                auto& row = queue_->get_writing_row();
                if (row.size() > cindex_) {
                    row.at(cindex_) = column_string;
                } else {
                    row.emplace_back(column_string);
                }
            }
            catch(const SharedMemoryException& ex) {
                return ogawayama::stub::ErrorCode::TIMEOUT;
            }
            cindex_++;
            return ogawayama::stub::ErrorCode::OK;
        }
        void resize_writing_row(std::size_t n) {
            queue_->get_writing_row().resize(n);
        }
        /**
         * @brief push the row into the queue.
         */
        ogawayama::stub::ErrorCode push_writing_row() {
            while (true) {
                auto retv = queue_->push();
                if (retv != ogawayama::stub::ErrorCode::TIMEOUT) {
                    cindex_ = 0;
                    queue_->get_writing_row().clear();
                    return retv;
                }
                if (!is_alive()) {
                    return ogawayama::stub::ErrorCode::SERVER_FAILURE;
                }
            }
        }
        /**
         * @brief get current column index.
         */
        auto get_cindex() const {
            return cindex_;
        }

        /**
         * @brief get the current row.
         * @return reference to the current row
         */
        ShmRowArgs & get_current_row() const {
            return queue_->get_current_row();
        }
        
        /**
         * @brief move current to the next in the queue.
         */
        ogawayama::stub::ErrorCode next() {
            while (true) {
                auto retv = queue_->pop();
                if (retv != ogawayama::stub::ErrorCode::TIMEOUT) {
                    remaining_--;
                    return retv;
                }
                if (!is_alive()) {
                    return ogawayama::stub::ErrorCode::SERVER_FAILURE;
                }
            }
        }

        void push_type(ogawayama::stub::Metadata::ColumnType::Type type, std::size_t length) {
            queue_->push_type(type, length);
        }

        auto get_metadata_ptr() const {
            return queue_->get_metadata_ptr();
        }

        bool is_alive() {
            if (owner_) {
                return queue_->is_partner();
            }
            if (shared_memory_->is_alive()) {
                try {
                    return shared_memory_->get_managed_shared_memory_ptr()->find<SpscQueue>(name_).first != nullptr;
                }
                catch(const boost::interprocess::interprocess_exception& ex) {
                    return false;
                }
            }
            return false;
        }
        void set_bulk_transfer_mode(std::size_t numerator = 1, std::size_t denominator = 2) {
            remaining_ = queue_->get_capacity() - 1;
            threshold_ = (remaining_ * numerator) / denominator;
            queue_->set_requested(remaining_);
        }
        bool is_need_next() {
            if (threshold_ == 0) { remaining_++; return true; } // multiple rows transfer is not used
            if (remaining_ > threshold_ || is_eor()) { return false; }
            std::size_t requested = queue_->get_capacity() - threshold_ - 1;
            remaining_ += requested;
            queue_->set_requested(requested);
            return true;
        }
        std::size_t get_requested() { return queue_->get_requested(); }

        /**
         * @brief initialize the SpscQueue, for reuse.
         */
        void clear() {
            queue_->clear();
        }

        void set_eor() { queue_->set_eor(); }
        bool is_eor() const { return queue_->is_eor(); }
        void set_closed() { queue_->set_closed(); }
        bool is_closed() const { return queue_->is_closed(); }

    private:
        SharedMemory *shared_memory_;
        SpscQueue *queue_;
        const bool owner_;
        char name_[param::MAX_NAME_LENGTH]{};
        std::size_t cindex_{};
        std::size_t remaining_{0};
        std::size_t threshold_{0};
    };
    
};  // namespace ogawayama::common
