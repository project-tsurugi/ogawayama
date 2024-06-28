/*
 * Copyright 2019-2023 Project Tsurugi.
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

#include <set>
#include <mutex>
#include <condition_variable>
#include <string>
#include <queue>
#include <thread>

#include <glog/logging.h>
#include <tateyama/logging.h>

#include "tateyama/transport/wire.h"

namespace tateyama::common::server_wire {
using namespace tateyama::common::wire;

class server_wire_container
{
    static constexpr std::size_t shm_size = ((1<<16) * 16 * 16);  // 16M (= 64K * 16writes * 16result_sets) (tentative)  NOLINT
    static constexpr std::size_t request_buffer_size = (1<<12);   //  4K bytes NOLINT
    static constexpr std::size_t response_buffer_size = (1<<13);  //  8K bytes NOLINT
    static constexpr std::size_t resultset_vector_size = (1<<12); //  4K bytes NOLINT
    static constexpr std::size_t writer_count = 16;
    static constexpr std::size_t resultset_buffer_size = (1<<16); //  64K bytes NOLINT

public:
    class resultset_wires_container;

    // resultset_wire_container
    class resultset_wire_container {
    public:
        resultset_wire_container(shm_resultset_wire* resultset_wire, resultset_wires_container& resultset_wires_container)
            : shm_resultset_wire_(resultset_wire), resultset_wires_container_(resultset_wires_container) {
        }
        ~resultset_wire_container() {
            if (thread_active_) {
                if(writer_thread_.joinable()) {
                    writer_thread_.join();
                }
            }
        }

        /**
         * @brief Copy and move constructers are delete.
         */
        resultset_wire_container(resultset_wire_container const&) = delete;
        resultset_wire_container(resultset_wire_container&&) = delete;
        resultset_wire_container& operator = (resultset_wire_container const&) = delete;
        resultset_wire_container& operator = (resultset_wire_container&&) = delete;

        void operator()() {
            while (true) {
                std::unique_lock<std::mutex> lock_queue(mtx_queue_);
                if (queue_.empty() && !released_) {
                    cnd_.wait(lock_queue, [this]{ return !queue_.empty() || released_; });
                }
                if (queue_.empty() && released_) {
                    annex_.clear();
                    write_complete();
                    return;
                }
                std::size_t chunk_size = queue_.front();
                queue_.pop();
                lock_queue.unlock();
                if (shm_resultset_wire_->wait_room(chunk_size)) {
                    annex_.clear();
                    return;
                }
                if (chunk_size > 0) {
                    std::unique_lock<std::mutex> lock_buffer(mtx_buffer_);
                    shm_resultset_wire_->write(annex_.substr(read_pos_).data(), chunk_size);
                }
                shm_resultset_wire_->flush();
                read_pos_ += chunk_size;
            }
        }
        void write(char const* data, std::size_t length) {
            if (!annex_mode_) {
                if (shm_resultset_wire_->check_room(length)) {
                    shm_resultset_wire_->write(data, length);
                    return;
                }
                annex_mode_ = true;
                write_pos_ = 0;
                read_pos_ = 0;
                chunk_size_ = 0;
                if (!thread_active_) {
                    writer_thread_ = std::thread(std::ref(*this));
                    thread_active_ = true;
                }
            }
            if (annex_.capacity() < (write_pos_ + chunk_size_ + length)) {
                std::size_t size = ((annex_.capacity() / resultset_buffer_size) + 1) * resultset_buffer_size;
                {
                    std::unique_lock<std::mutex> lock(mtx_buffer_);
                    annex_.reserve(size);
                }
            }
            annex_.insert(write_pos_ + chunk_size_, data, length);
            chunk_size_ += length;
        }
        void flush() {
            if (!annex_mode_) {
                shm_resultset_wire_->flush();
                return;
            }
            if (chunk_size_ > 0) {
                std::unique_lock<std::mutex> lock(mtx_queue_);
                queue_.push(chunk_size_);
                cnd_.notify_one();
            }
            write_pos_ += chunk_size_;
            chunk_size_ = 0;
        }

    private:
        shm_resultset_wire* shm_resultset_wire_;
        resultset_wires_container &resultset_wires_container_;

        std::string annex_{};
        std::queue<std::size_t> queue_{};
        std::thread writer_thread_{};
        std::size_t write_pos_{};
        std::size_t chunk_size_{};
        std::size_t read_pos_{};
        bool annex_mode_{};
        bool thread_active_{};
        bool released_{};

        std::mutex mtx_queue_{};
        std::mutex mtx_buffer_{};
        std::condition_variable cnd_{};

        void write_complete();
    };
    using unq_p_resultset_wire_conteiner = std::unique_ptr<resultset_wire_container>;

    // resultset_wires_container
    class resultset_wires_container {
    public:
        //   for server
        resultset_wires_container(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::string_view name, std::size_t count, std::mutex& mtx_shm)
            : managed_shm_ptr_(managed_shm_ptr), rsw_name_(name), server_(true), mtx_shm_(mtx_shm) {
            std::lock_guard<std::mutex> lock(mtx_shm_);
            managed_shm_ptr_->destroy<shm_resultset_wires>(rsw_name_.c_str());
            try {
                shm_resultset_wires_ = managed_shm_ptr_->construct<shm_resultset_wires>(rsw_name_.c_str())(managed_shm_ptr_, count, resultset_buffer_size);
            } catch(const boost::interprocess::interprocess_exception& ex) {
                LOG(ERROR) << ex.what() << " on resultset_wires_container::resultset_wires_container()";
                pthread_exit(nullptr);  // FIXME
            } catch (std::runtime_error &ex) {
                LOG(ERROR) << "running out of boost managed shared memory";
                pthread_exit(nullptr);  // FIXME
            }
        }
        //  constructor for client
        resultset_wires_container(boost::interprocess::managed_shared_memory* managed_shm_ptr, std::string_view name, std::mutex& mtx_shm)
            : managed_shm_ptr_(managed_shm_ptr), rsw_name_(name), server_(false), mtx_shm_(mtx_shm) {
            shm_resultset_wires_ = managed_shm_ptr_->find<shm_resultset_wires>(rsw_name_.c_str()).first;
            if (shm_resultset_wires_ == nullptr) {
                throw std::runtime_error("cannot find the resultset wire");
            }
        }
        ~resultset_wires_container() {
            if (server_) {
                std::lock_guard<std::mutex> lock(mtx_shm_);
                managed_shm_ptr_->destroy<shm_resultset_wires>(rsw_name_.c_str());
            }
        }

        /**
         * @brief Copy and move constructers are delete.
         */
        resultset_wires_container(resultset_wires_container const&) = delete;
        resultset_wires_container(resultset_wires_container&&) = delete;
        resultset_wires_container& operator = (resultset_wires_container const&) = delete;
        resultset_wires_container& operator = (resultset_wires_container&&) = delete;

        unq_p_resultset_wire_conteiner acquire() {
            std::lock_guard<std::mutex> lock(mtx_shm_);
            try {
                return std::make_unique<resultset_wire_container>(shm_resultset_wires_->acquire(), *this);
            } catch(const boost::interprocess::interprocess_exception& ex) {
                LOG(ERROR) << ex.what() << " on resultset_wires_container::acquire()";
                pthread_exit(nullptr);  // FIXME
            } catch (std::runtime_error &ex) {
                LOG(ERROR) << "running out of boost managed shared memory";
                pthread_exit(nullptr);  // FIXME
            }
        }

        void set_eor() {
            if (deffered_writers_ == 0) {
                shm_resultset_wires_->set_eor();
            }
        }
        bool is_closed() {
            return shm_resultset_wires_->is_closed();
        }

        void add_deffered_delete(unq_p_resultset_wire_conteiner resultset_wire) {
            deffered_delete_.emplace(std::move(resultset_wire));
            deffered_writers_++;
        }
        void write_complete() {
            deffered_writers_--;
            if (deffered_writers_ == 0) {
                shm_resultset_wires_->set_eor();
            }
        }

        // for client
        std::string_view get_chunk() {
            if (wrap_around_.data()) {
                auto rv = wrap_around_;
                wrap_around_ = std::string_view();
                return rv;
            }
            if (current_wire_ == nullptr) {
                current_wire_ = active_wire();
            }
            if (current_wire_ != nullptr) {
                return current_wire_->get_chunk(current_wire_->get_bip_address(managed_shm_ptr_), wrap_around_);
            }
            return std::string_view();
        }
        void dispose(std::size_t) {
            if (current_wire_ != nullptr) {
                current_wire_->dispose(current_wire_->get_bip_address(managed_shm_ptr_));
                current_wire_ = nullptr;
                return;
            }
            std::abort();
        }
        bool is_eor() {
            return shm_resultset_wires_->is_eor();
        }

    private:
        boost::interprocess::managed_shared_memory* managed_shm_ptr_;
        std::string rsw_name_;
        shm_resultset_wires* shm_resultset_wires_{};
        bool server_;
        std::mutex& mtx_shm_;
        std::set<unq_p_resultset_wire_conteiner> deffered_delete_{};
        std::size_t deffered_writers_{};

        //   for client
        std::string_view wrap_around_{};
        shm_resultset_wire* current_wire_{};

        shm_resultset_wire* active_wire() {
            return shm_resultset_wires_->active_wire();
        }
    };
    using unq_p_resultset_wires_conteiner = std::unique_ptr<resultset_wires_container>;

    class wire_container {
    public:
        wire_container() = default;
        ~wire_container() = default;
        wire_container(wire_container const&) = delete;
        wire_container(wire_container&&) = delete;
        wire_container& operator = (wire_container const&) = delete;
        wire_container& operator = (wire_container&&) = delete;

        void initialize(unidirectional_message_wire* wire, char* bip_buffer) {
            wire_ = wire;
            bip_buffer_ = bip_buffer;
        }
        message_header peep() {
            return wire_->peep(bip_buffer_);
        }
        std::string_view payload() {
            return wire_->payload(bip_buffer_);
        }
        void read(char* to) {
            wire_->read(to, bip_buffer_);
        }
        std::size_t read_point() { return wire_->read_point(); }
        void dispose() { wire_->dispose(); }

        // for client
        void write(const char* from, const std::size_t len, message_header::index_type index) {
            wire_->write(bip_buffer_, from, message_header(index, len));
        }

    private:
        unidirectional_message_wire* wire_{};
        char* bip_buffer_{};
    };

    class response_wire_container {
    public:
        response_wire_container() = default;
        ~response_wire_container() = default;
        response_wire_container(response_wire_container const&) = delete;
        response_wire_container(response_wire_container&&) = delete;
        response_wire_container& operator = (response_wire_container const&) = delete;
        response_wire_container& operator = (response_wire_container&&) = delete;

        void initialize(unidirectional_response_wire* wire, char* bip_buffer) {
            wire_ = wire;
            bip_buffer_ = bip_buffer;
        }
        void write(const char* from, response_header header) {
            std::lock_guard<std::mutex> lock(mtx_);
            wire_->write(bip_buffer_, from, header);
        }

        // for client
        response_header await() {
            return wire_->await(bip_buffer_);
        }
        [[nodiscard]] response_header::length_type get_length() const {
            return wire_->get_length();
        }
        [[nodiscard]] response_header::index_type get_idx() const {
            return wire_->get_idx();
        }
        [[nodiscard]] response_header::msg_type get_type() const {
            return wire_->get_type();
        }
        void read(char* to) {
            wire_->read(to, bip_buffer_);
        }
        void close() {
            wire_->close();
        }

    private:
        unidirectional_response_wire* wire_{};
        char* bip_buffer_{};
        std::mutex mtx_{};
    };

    explicit server_wire_container(std::string_view name) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), shm_size);
            auto req_wire = managed_shared_memory_->construct<unidirectional_message_wire>(request_wire_name)(managed_shared_memory_.get(), request_buffer_size);
            auto res_wire = managed_shared_memory_->construct<unidirectional_response_wire>(response_wire_name)(managed_shared_memory_.get(), response_buffer_size);

            request_wire_.initialize(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()));
            response_wire_.initialize(res_wire, res_wire->get_bip_address(managed_shared_memory_.get()));
        } catch(const boost::interprocess::interprocess_exception& ex) {
            LOG(ERROR) << ex.what() << " on server_wire_container::server_wire_container()";
            pthread_exit(nullptr);  // FIXME
        } catch (std::runtime_error &ex) {
            LOG(ERROR) << "running out of boost managed shared memory";
            pthread_exit(nullptr);  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    server_wire_container(server_wire_container const&) = delete;
    server_wire_container(server_wire_container&&) = delete;
    server_wire_container& operator = (server_wire_container const&) = delete;
    server_wire_container& operator = (server_wire_container&&) = delete;

    ~server_wire_container() {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
    }

    wire_container* get_request_wire() { return &request_wire_; }
    response_wire_container& get_response_wire() { return response_wire_; }

    unq_p_resultset_wires_conteiner create_resultset_wires(std::string_view name, std::size_t count) {
        try {
            return std::make_unique<resultset_wires_container>(managed_shared_memory_.get(), name, count, mtx_shm_);
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            LOG(ERROR) << "running out of boost managed shared memory";
            pthread_exit(nullptr);  // FIXME
        }
    }
    unq_p_resultset_wires_conteiner create_resultset_wires(std::string_view name) {
        return create_resultset_wires(name, writer_count);
    }
    void close_session() { session_closed_ = true; }

    [[nodiscard]] bool is_session_closed() const { return session_closed_; }

    // for client
    std::unique_ptr<resultset_wires_container> create_resultset_wires_for_client(std::string_view name) {
        return std::make_unique<resultset_wires_container>(managed_shared_memory_.get(), name, mtx_shm_);
    }

private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    wire_container request_wire_{};
    response_wire_container response_wire_{};
    bool session_closed_{false};
    std::mutex mtx_shm_{};
};

inline void server_wire_container::resultset_wire_container::write_complete() {
    resultset_wires_container_.write_complete();
}

class connection_container
{
    static constexpr std::size_t request_queue_size = (1<<15);  // 32K bytes (tentative)  NOLINT

public:
    explicit connection_container(std::string_view name, std::size_t n) : name_(name) {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
        try {
            managed_shared_memory_ =
                std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::create_only, name_.c_str(), request_queue_size);
            managed_shared_memory_->destroy<connection_queue>(connection_queue::name);
            connection_queue_ = managed_shared_memory_->construct<connection_queue>(connection_queue::name)(n, managed_shared_memory_->get_segment_manager(), 1);
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            std::abort();  // FIXME
        }
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    connection_container(connection_container const&) = delete;
    connection_container(connection_container&&) = delete;
    connection_container& operator = (connection_container const&) = delete;
    connection_container& operator = (connection_container&&) = delete;

    ~connection_container() {
        boost::interprocess::shared_memory_object::remove(name_.c_str());
    }
    connection_queue& get_connection_queue() {
        return *connection_queue_;
    }

private:
    std::string name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    connection_queue* connection_queue_;
};

};  // namespace tateyama::common::server_wire
