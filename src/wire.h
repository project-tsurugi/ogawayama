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

#include <memory>
#include <exception>
#include <atomic>
#include <stdexcept> // std::runtime_error
#include <vector>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>

namespace tsubakuro::common::wire {

/**
 * @brief header information used in request message,
 * it assumes that machines with the same endianness communicate with each other
 */
class message_header {
public:
    using length_type = std::uint16_t;
    using index_type = std::uint16_t;
    static constexpr index_type not_use = 0xffff;

    static constexpr std::size_t size = sizeof(length_type) + sizeof(index_type);
    
    message_header(index_type idx, length_type length) : idx_(idx), length_(length) {}
    message_header() : message_header(0, 0) {}
    explicit message_header(const char* buffer) {
        std::memcpy(&idx_, buffer, sizeof(index_type));
        std::memcpy(&length_, buffer + sizeof(index_type), sizeof(length_type));
    }

    length_type get_length() const { return length_; }
    index_type get_idx() const { return idx_; }
    char* get_buffer() {
        std::memcpy(buffer_, &idx_, sizeof(index_type));
        std::memcpy(buffer_ + sizeof(index_type), &length_, sizeof(length_type));
        return buffer_;
    };

private:
    index_type idx_;
    length_type length_;
    char buffer_[size];
};

/**
 * @brief header information used in metadata message,
 * it assumes that machines with the same endianness communicate with each other
 */
class length_header {
public:
    using length_type = std::uint16_t;

    static constexpr std::size_t size = sizeof(length_type);
    
    explicit length_header(length_type length) : length_(length) {}
    explicit length_header(std::size_t length) : length_(static_cast<length_type>(length)) {}
    length_header() : length_header(static_cast<length_type>(0)) {}
    explicit length_header(const char* buffer) {
        std::memcpy(&length_, buffer, sizeof(length_type));
    }

    length_type get_length() const { return length_; }
    char* get_buffer() {
        std::memcpy(buffer_, &length_, sizeof(length_type));
        return buffer_;
    };

private:
    length_type length_;
    char buffer_[size];
};


static constexpr const char* request_wire_name = "request_wire";
static constexpr const char* response_box_name = "response_box";

/**
 * @brief One-to-one unidirectional communication of charactor stream with header T
 */
template <typename T>
class simple_wire
{
public:
    static constexpr std::size_t Alignment = 64;

    /**
     * @brief Construct a new object.
     */
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity)
        : managed_shm_ptr_(managed_shm_ptr), capacity_(capacity) {
        auto buffer = static_cast<char*>(managed_shm_ptr_->allocate_aligned(capacity_, Alignment));
        buffer_handle_ = managed_shm_ptr->get_handle_from_address(buffer);
    }

    /**
     * @brief Construct a new object.
     * for result_set_wire
     */
    simple_wire() : managed_shm_ptr_(nullptr), capacity_(0) {
        buffer_handle_ = 0;
    }
    simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr)
        : managed_shm_ptr_(managed_shm_ptr), capacity_(0) {
        buffer_handle_ = 0;
    }
    void set_managed_shared_memory(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
        managed_shm_ptr_ = managed_shm_ptr;
    }

    void attach_buffer(boost::interprocess::managed_shared_memory::handle_t handle, std::size_t capacity) {
        buffer_handle_ = handle;
        capacity_ = capacity;
    }
    void detach_buffer() {
        buffer_handle_ = 0;
        capacity_ = 0;
    }
    bool equal(boost::interprocess::managed_shared_memory::handle_t handle) {
        return handle == buffer_handle_;
    }
    
    /**
     * @brief Copy and move constructers are delete.
     */
    simple_wire(simple_wire const&) = delete;
    simple_wire(simple_wire&&) = delete;
    simple_wire& operator = (simple_wire const&) = delete;
    simple_wire& operator = (simple_wire&&) = delete;
            
    /**
     * @brief push the request message into the queue.
     */
    void write(char* base, const char* from, T&& header) {
        std::size_t length = header.get_length() + T::size;
        if (length > room()) { wait_to_write(length); }
        write_in_buffer(base, point(base, pushed_), header.get_buffer(), T::size);
        write_in_buffer(base, point(base, pushed_ + T::size), from, header.get_length());
        pushed_ += length;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_read_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_empty_.notify_one();
        }
    }
    void write(const char* from, T&& header) {
        write(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), from, std::move(header));
    }

    /**
     * @brief peep the current header.
     */
    T peep(const char* base, bool wait_flag = false) {
        while (true) {
            if(data_length() >= T::size) {
                break;
            }
            if (wait_flag) {
                boost::interprocess::scoped_lock lock(m_mutex_);
                wait_for_read_ = true;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                c_empty_.wait(lock, [this](){ return data_length() >= T::size; });
                wait_for_read_ = false;
            } else {
                if (data_length() < T::size) { return T(); }
            }
        }
        if ((base + capacity_) >= (read_point(base) + sizeof(T))) {
            T header(read_point(base));  // normal case
            return header;
        }
        char buf[sizeof(T)];  // in case for ring buffer full
        std::size_t first_part = capacity_ - index(poped_);
        memcpy(buf, read_point(base), first_part);
        memcpy(buf + first_part, base, sizeof(T) - first_part);
        T header(buf);
        return header;
    }
    T peep(bool wait_flag = false) {
        return peep(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), wait_flag);
    }

    const char* payload(const char* base, std::size_t length) {
        if (index(poped_ + T::size) < index(poped_ + T::size + length)) {
            return read_point(base, T::size);
        }
        copy_of_payload_ = static_cast<char*>(malloc(length));
        read_from_buffer(copy_of_payload_, base, read_point(base, T::size), length);
        return copy_of_payload_;  // ring buffer wrap around case
    }
    const char* payload(std::size_t length) {
        return payload(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), length);
    }

    /**
     * @brief pop the current message.
     */
    void read(char* to, const char* base, std::size_t length) {
        read_from_buffer(to, base, read_point(base, T::size), length);
        poped_ += T::size + length;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_write_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_full_.notify_one();
        }
    }
    void read(char* to, std::size_t length) {
        read(to, static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), length);
    }

    std::size_t data_length() { return (pushed_ - poped_); }

    char* get_bip_address(boost::interprocess::managed_shared_memory* managed_shm_ptr) {
        if (buffer_handle_ != 0) {
            return static_cast<char*>(managed_shm_ptr->get_address_from_handle(buffer_handle_));
        }
        return nullptr;
    }

    std::size_t read_point() const { return poped_; }

    /**
     * @brief dispose of data that has completed read and is no longer needed
     *  used by endpoint IF
     */
    void dispose(const char* base, const std::size_t rp) {
        if (poped_ == rp) {
            poped_ += peep(base).get_length();
            if (copy_of_payload_ != nullptr) {
                free(copy_of_payload_);
                copy_of_payload_ = nullptr;
            }
        }
    }
    void dispose(const std::size_t rp) {
        dispose(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), rp);
    }

protected:
    std::size_t index(std::size_t n) const { return n %  capacity_; }
    const char* read_point(const char* base) const { return base + index(poped_); }
    const char* read_point(const char* base, std::size_t offset) const { return base + index(poped_ + offset); }
    char* point(char* base, std::size_t i) { return base + index(i); }
    void wait_to_write(std::size_t length) {
        boost::interprocess::scoped_lock lock(m_mutex_);
        wait_for_write_ = true;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        c_full_.wait(lock, [this, length](){ return room() >= length; });
        wait_for_write_ = false;
    }
    void write_in_buffer(char *base, char* to, const char* from, std::size_t length) {
        if((base + capacity_) >= (to + length)) {
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (to - base);
            memcpy(to, from, first_part);
            memcpy(base, from + first_part, length - first_part);
        }
    }
    void read_from_buffer(char* to, const char *base, const char* from, std::size_t length) const {
        if((base + capacity_) >= (from + length)) {
            memcpy(to, from, length);
        } else {
            std::size_t first_part = capacity_ - (from - base);
            memcpy(to, from, first_part);
            memcpy(to + first_part, base, length - first_part);
        }
    }

    boost::interprocess::managed_shared_memory* managed_shm_ptr_;
    boost::interprocess::managed_shared_memory::handle_t buffer_handle_{};
    std::size_t capacity_;

    std::size_t pushed_{0};
    std::size_t poped_{0};

private:
    std::size_t room() const { return capacity_ - (pushed_ - poped_); }

    std::atomic_bool wait_for_write_{};
    std::atomic_bool wait_for_read_{};

    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_empty_{};
    boost::interprocess::interprocess_condition c_full_{};

    char *copy_of_payload_{};  // in case of ring buffer wrap around
};


// for request
class unidirectional_message_wire : public simple_wire<message_header> {
public:
    unidirectional_message_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t capacity) : simple_wire<message_header>(managed_shm_ptr, capacity) {}
};


// for response
class response_box {
public:

    class response {
    public:
        static constexpr std::size_t max_response_message_length = 256;

        response() : nstored_(0) {};

        /**
         * @brief Copy and move constructers are deleted.
         */
        response(response const&) = delete;
        response(response&&) = delete;
        response& operator = (response const&) = delete;
        response& operator = (response&&) = delete;

        std::pair<char*, std::size_t> recv() {
            nstored_.wait();
            return std::pair<char*, std::size_t>(buffer_, length_);
        }
        void set_inuse() {
            inuse_ = true;
        }
        bool is_inuse() { return inuse_; }
        void dispose() {
            length_ = 0;
            inuse_ = false;
        }
        char* get_buffer() {
            return buffer_;
        }
        void flush(std::size_t length) {
            length_ = length;
            nstored_.post();
        }
        char* get_buffer(std::size_t length) {
            length_ = length;
            return buffer_;
        }
        void flush() {
            nstored_.post();
        }

    private:
        std::size_t length_{};
        bool inuse_{};
        bool waiter_{};

        boost::interprocess::interprocess_semaphore nstored_;
        char buffer_[max_response_message_length];
    };

    /**
     * @brief Construct a new object.
     */
    explicit response_box(size_t aSize, boost::interprocess::managed_shared_memory* managed_shm_ptr) : mData_(aSize, managed_shm_ptr->get_segment_manager()) {}
    response_box() = delete;

    /**
     * @brief Copy and move constructers are deleted.
     */
    response_box(response_box const&) = delete;
    response_box(response_box&&) = delete;
    response_box& operator = (response_box const&) = delete;
    response_box& operator = (response_box&&) = delete;

    std::size_t size() { return mData_.size(); }
    response& at(std::size_t idx) { return mData_.at(idx); }

private:
    std::vector<response, boost::interprocess::allocator<response, boost::interprocess::managed_shared_memory::segment_manager>> mData_;
};


// for resultset
class unidirectional_simple_wire : public simple_wire<length_header> {
public:
    unidirectional_simple_wire() : simple_wire<length_header>() {}
    unidirectional_simple_wire(boost::interprocess::managed_shared_memory* managed_shm_ptr) : simple_wire<length_header>(managed_shm_ptr) {}

    /**
     * @brief Copy and move constructers are deleted.
     */
    unidirectional_simple_wire(unidirectional_simple_wire const&) = delete;
    unidirectional_simple_wire(unidirectional_simple_wire&&) = delete;
    unidirectional_simple_wire& operator = (unidirectional_simple_wire const&) = delete;
    unidirectional_simple_wire& operator = (unidirectional_simple_wire&&) = delete;
    
    /**
     * @brief push record into the queue.
     */
    void write(char* base, const char* from, std::size_t length) {
        if (length > room()) { wait_to_write(length); }
        if (piled_ == 0) {
            length_header h(static_cast<length_header::length_type>(0));
            write_in_buffer(base, point(base, pushed_), h.get_buffer(), length_header::size);            
        }
        write_in_buffer(base, point(base, pushed_ + length_header::size + piled_), from, length);
        piled_ += length;
    }
    void write(const char* from, std::size_t length) {
        write(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), from, length);
    }

    void commit(char* base) {
        if (piled_ == 0) {
            eor_ = true;
        }
        length_header h(static_cast<length_header::length_type>(piled_));
        write_in_buffer(base, point(base, pushed_), h.get_buffer(), length_header::size);
        n_records_++;
        pushed_ += length_header::size + piled_;
        piled_ = 0;
        if (wait_for_record_) {
            boost::interprocess::scoped_lock lock(r_mutex_);
            r_condition_.notify_one();
        }
    }
    void commit() {
        commit(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)));
    }
        
    /**
     * @brief provide the current chunk to MsgPack.
     */
    std::pair<char*, std::size_t> get_chunk(char* base, bool wait_flag) {
        if (!has_record() && wait_flag) {
            boost::interprocess::scoped_lock lock(r_mutex_);
            wait_for_record_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            r_condition_.wait(lock, [this](){ return has_record(); });
            wait_for_record_ = false;
        }

        std::size_t length = peep(base).get_length();
        std::size_t chunk_start = poped_ + length_header::size;
        std::size_t seq_length = capacity_ - (chunk_start % capacity_);
        if (chunk_resume_ != 0) {
            auto resume_point = chunk_resume_;
            chunk_resume_ = 0;
            return std::pair<char*, std::size_t>(point(base, resume_point), length - seq_length);
        }
        if (seq_length >= length) {
            return std::pair<char*, std::size_t>(point(base, chunk_start), length);
        }
        chunk_resume_ = chunk_start + seq_length;
        return std::pair<char*, std::size_t>(point(base, chunk_start), seq_length);
    }
    std::pair<char*, std::size_t> get_chunk(bool wait_flag) {
        return get_chunk(static_cast<char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)), wait_flag);
    }
    bool has_record() { return n_records_ > 0; }
    void dispose(const char* base) {
        poped_ += (length_header::size + peep(base).get_length());
        n_records_--;
    }
    void dispose() {
        dispose(static_cast<const char*>(managed_shm_ptr_->get_address_from_handle(buffer_handle_)));
    }
    bool is_eor() { return eor_; }

private:
    std::size_t room() const { return capacity_ - ((pushed_ + length_header::size + piled_) - poped_); }
    
    std::size_t n_records_{0};
    std::size_t piled_{0};
    std::size_t chunk_resume_{0};
    bool eor_{false};

    std::atomic_bool wait_for_record_{};
    boost::interprocess::interprocess_mutex r_mutex_{};
    boost::interprocess::interprocess_condition r_condition_{};
};

using shm_resultset_wire = unidirectional_simple_wire;

class unidirectional_simple_wires {
public:
    static constexpr std::size_t wire_size = (1<<16);  // 64K bytes (tentative)
    static constexpr std::size_t Alignment = 64;
    using allocator = boost::interprocess::allocator<shm_resultset_wire, boost::interprocess::managed_shared_memory::segment_manager>;
    
    unidirectional_simple_wires(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::size_t count)
        : managed_shm_ptr_(managed_shm_ptr), unidirectional_simple_wires_(count, managed_shm_ptr->get_segment_manager()) {
        for (auto&& wire: unidirectional_simple_wires_) {
            wire.set_managed_shared_memory(managed_shm_ptr);
        }
        reserved_ = static_cast<char*>(managed_shm_ptr->allocate_aligned(wire_size, Alignment));
    }
    ~unidirectional_simple_wires() {
        if (reserved_ != nullptr) {
            managed_shm_ptr_->deallocate(reserved_);
        }
        for (auto&& wire: unidirectional_simple_wires_) {
            if (!wire.equal(0)) {
                managed_shm_ptr_->deallocate(wire.get_bip_address(managed_shm_ptr_));
                wire.detach_buffer();
            }
        }
    }

    shm_resultset_wire* acquire() {
        if (count_using_ == 0) {
            count_using_ = next_index_ = 1;
            unidirectional_simple_wires_.at(0).attach_buffer(managed_shm_ptr_->get_handle_from_address(reserved_), wire_size);
            reserved_ = nullptr;
            only_one_buffer_ = true;
            return &unidirectional_simple_wires_.at(0);
        }

        char* buffer;
        if (reserved_ != nullptr) {
            buffer = reserved_;
            reserved_ = nullptr;
        } else {
            buffer = static_cast<char*>(managed_shm_ptr_->allocate_aligned(wire_size, Alignment));
        }
        auto index = search_free_wire();
        unidirectional_simple_wires_.at(index).attach_buffer(managed_shm_ptr_->get_handle_from_address(buffer), wire_size);
        only_one_buffer_ = false;
        return &unidirectional_simple_wires_.at(index);
    }
    void release(shm_resultset_wire* wire) {
        char* buffer = wire->get_bip_address(managed_shm_ptr_);

        if (reserved_ == nullptr) {
            reserved_ = buffer;
        } else {
            managed_shm_ptr_->deallocate(buffer);
        }
        count_using_--;
        return;
    }
    
    shm_resultset_wire* search() {
        if (only_one_buffer_) {
            return &unidirectional_simple_wires_.at(0);
        }
        for (auto&& wire: unidirectional_simple_wires_) {
            if(wire.has_record()) {
                return &wire;
            }
        }
        return nullptr;
    }

private:
    std::size_t search_free_wire() {
        if (count_using_ == next_index_) {
            count_using_++;
            return next_index_++;
        }
        for (std::size_t index = 0; index < next_index_; index++) {
            if (unidirectional_simple_wires_.at(index).equal(0)) {
                count_using_++;
                return index;
            }
        }
        std::abort();  // FIXME
    }

    boost::interprocess::managed_shared_memory* managed_shm_ptr_;
    std::vector<shm_resultset_wire, allocator> unidirectional_simple_wires_;

    char* reserved_{};
    std::size_t count_using_{};
    std::size_t next_index_{};
    bool only_one_buffer_{};
};

using shm_resultset_wires = unidirectional_simple_wires;


// implements connect operation
class connection_queue
{
public:
    constexpr static const char* name = "connection_queue";

    /**
     * @brief Construct a new object.
     */
    connection_queue() = default;

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_queue(connection_queue const&) = delete;
    connection_queue(connection_queue&&) = delete;
    connection_queue& operator = (connection_queue const&) = delete;
    connection_queue& operator = (connection_queue&&) = delete;

    std::size_t request() {
        std::size_t rv;

        rv = ++requested_;
        std::atomic_thread_fence(std::memory_order_acq_rel);
        if (wait_for_request_) {
            boost::interprocess::scoped_lock lock(m_mutex_);
            c_requested_.notify_one();
        }
        return rv;
    }
    bool check(std::size_t n, bool wait = false) {
        if (!wait) {
            return accepted_ >= n;
        } else if (accepted_ >= n) {
            return true;
        }
        {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_accept_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_accepted_.wait(lock, [this, n](){ return (accepted_ >= n); });
            wait_for_accept_ = false;
        }
        return true;
    }
    std::size_t listen(bool wait = false) {
        if (accepted_ < requested_) {
            return accepted_ + 1;
        }
        if (!wait) {
            return 0;
        }
        {
            boost::interprocess::scoped_lock lock(m_mutex_);
            wait_for_request_ = true;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            c_requested_.wait(lock, [this](){ return (accepted_ < requested_); });
            wait_for_request_ = false;
        }
        return accepted_ + 1;
    }
    void accept(std::size_t n) {
        if (n == (accepted_ + 1)) {
            if (n <= requested_) {
                accepted_ = n;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                if (wait_for_accept_) {
                    boost::interprocess::scoped_lock lock(m_mutex_);
                    c_accepted_.notify_all();
                }
                return;
            } else {
                throw std::runtime_error("Received an session id that was not requested for connection");
            }
        }
        throw std::runtime_error("The session id is not sequential");
    }

private:
    std::size_t requested_{0};
    std::size_t accepted_{0};
    std::atomic_bool wait_for_request_{};
    std::atomic_bool wait_for_accept_{};
    boost::interprocess::interprocess_mutex m_mutex_{};
    boost::interprocess::interprocess_condition c_requested_{};
    boost::interprocess::interprocess_condition c_accepted_{};
};

};  // namespace tsubakuro::common
