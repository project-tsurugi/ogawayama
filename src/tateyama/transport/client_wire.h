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

#include <atomic>
#include <array>
#include <mutex>
#include <condition_variable>
#include <stdexcept> // std::runtime_error

#include "wire.h"

namespace tateyama::common::wire {

class session_wire_container
{
    static constexpr std::size_t metadata_size_boundary = 256;
    static constexpr std::size_t slot_size = 16;
    constexpr static tateyama::common::wire::response_header::msg_type RESPONSE_BODYHEAD = 2;

public:
    class resultset_wires_container {
    public:
        explicit resultset_wires_container(session_wire_container *envelope) noexcept
            : envelope_(envelope), managed_shm_ptr_(envelope_->managed_shared_memory_.get()) {
        }
        void connect(std::string_view name) {
            rsw_name_ = name;
            shm_resultset_wires_ = managed_shm_ptr_->find<shm_resultset_wires>(rsw_name_.c_str()).first;
            if (shm_resultset_wires_ == nullptr) {
                std::string msg("cannot find a result_set wire with the specified name: ");
                msg += name;
                throw std::runtime_error(msg.c_str());
            }
        }
        std::string_view get_chunk() {
            if (!wrap_around_.empty()) {
                wrap_around_.clear();
            }
            if (current_wire_ == nullptr) {
                current_wire_ = active_wire();
            }
            if (current_wire_ != nullptr) {
                while (true) {
                    try {
                        std::string_view extrusion{};
                        auto rtnv = current_wire_->get_chunk(current_wire_->get_bip_address(managed_shm_ptr_), extrusion);
                        if (extrusion.length() == 0) {
                            return rtnv;
                        }
                        wrap_around_ = rtnv;
                        wrap_around_ += extrusion;
                        return wrap_around_;
                    } catch (std::runtime_error &ex) {
                        if (auto err = envelope_->get_status_provider().is_alive(); !err.empty()) {
                            throw ex;  // FIXME handle this
                        }
                        continue;
                    }
                }
            }
            return {nullptr, 0};
        }
        void dispose() {
            if (current_wire_ != nullptr) {
                current_wire_->dispose(current_wire_->get_bip_address(managed_shm_ptr_));
                current_wire_ = nullptr;
                return;
            }
            std::abort();  //  This must not happen.
        }
        bool is_eor() noexcept {
            return shm_resultset_wires_->is_eor();
        }
        void set_closed() {
            shm_resultset_wires_->set_closed();
        }
        session_wire_container* get_envelope() noexcept {
            return envelope_;
        }

    private:
        shm_resultset_wire* active_wire() {
            return shm_resultset_wires_->active_wire();
        }

        session_wire_container *envelope_;
        boost::interprocess::managed_shared_memory* managed_shm_ptr_;
        std::string rsw_name_;
        shm_resultset_wires* shm_resultset_wires_{};
        //   for client
        shm_resultset_wire* current_wire_{};
        std::string wrap_around_{};
    };

    class request_wire_container {
    public:
        request_wire_container() = default;
        request_wire_container(unidirectional_message_wire* wire, char* bip_buffer) noexcept : wire_(wire), bip_buffer_(bip_buffer) {};
        message_header peep() {
            return wire_->peep(bip_buffer_);
        }
        void write(const std::string& data, message_header::index_type index) {
            wire_->write(bip_buffer_, data.data(), message_header(index, data.length()));
        }
        void disconnect() {
            wire_->terminate();
        }

    private:
        unidirectional_message_wire* wire_{};
        char* bip_buffer_{};
    };

    class response_wire_container {
    public:
        response_wire_container() = default;
        response_wire_container(session_wire_container* envelope, unidirectional_response_wire* wire, char* bip_buffer) noexcept
            : envelope_(envelope), wire_(wire), bip_buffer_(bip_buffer) {};
        [[nodiscard]] response_header::length_type get_length() const noexcept {
            return wire_->get_length();
        }
        [[nodiscard]] response_header::index_type get_idx() const noexcept {
            return wire_->get_idx();
        }
        [[nodiscard]] response_header::msg_type get_type() const noexcept {
            return wire_->get_type();
        }
        void read(char* top) {
            wire_->read(top, bip_buffer_);
        }
        void close() {
            wire_->close();
        }

    private:
        session_wire_container* envelope_{};
        unidirectional_response_wire* wire_{};
        char* bip_buffer_{};

        response_header await() {
            while (true) {
                try {
                    return wire_->await(bip_buffer_);
                } catch (std::runtime_error &ex) {
                    if (auto err = envelope_->get_status_provider().is_alive(); !err.empty()) {
                        throw ex;  // FIXME handle this
                    }
                    continue;
                }
            }
        }

        friend class session_wire_container;
    };

    class slot {
    public:
        slot() = default;

        bool test_and_set_in_use() {
            return in_use_.test_and_set();
        }
        bool valid() {
            return received_.load() > consumed_.load();
        }
        std::string& pre_receive(response_header::msg_type msg_type) {
            if (expected_ == 0) {
                expected_ = (msg_type == RESPONSE_BODYHEAD) ? 2 : 1;
            }
            if (expected_ == 2 && received_.load() == 0) {
                return body_head_message_;
            }
            return body_message_;
        }
        void post_receive() {
            std::atomic_thread_fence(std::memory_order_acq_rel);
            received_++;
        }
        void receive_and_consume(response_header::msg_type msg_type) {
            if (expected_ == 0) {
                expected_ = (msg_type == RESPONSE_BODYHEAD) ? 2 : 1;
            }
            if (expected_ == 2 && received_.load() == 0) {
                received_++;
                consumed_++;
            } else {
                finish_receive();
            }
        }
        void consume(std::string& message) {
            if (expected_ == 2 && consumed_.load() == 0) {
                message = body_head_message_;
                std::atomic_thread_fence(std::memory_order_acq_rel);
                consumed_++;
            } else {
                message = body_message_;
                finish_receive();
            }
        }

    private:
        std::atomic_flag in_use_{};
        std::atomic_int received_{};
        std::atomic_int consumed_{};
        std::int32_t expected_{};
        std::string body_message_{};
        std::string body_head_message_{};

        void finish_receive() {
            body_message_.clear();
            body_head_message_.clear();
            received_.store(0);
            consumed_.store(0);
            expected_ = 0;
            std::atomic_thread_fence(std::memory_order_acq_rel);
            in_use_.clear();
        }
    };

    explicit session_wire_container(std::string_view name) : db_name_(name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            auto req_wire = managed_shared_memory_->find<unidirectional_message_wire>(request_wire_name).first;
            auto res_wire = managed_shared_memory_->find<unidirectional_response_wire>(response_wire_name).first;
            status_provider_ = managed_shared_memory_->find<status_provider>(status_provider_name).first;
            if (req_wire == nullptr || res_wire == nullptr || status_provider_ == nullptr) {
                throw std::runtime_error("cannot find the session wire");
            }
            request_wire_ = request_wire_container(req_wire, req_wire->get_bip_address(managed_shared_memory_.get()));
            response_wire_ = response_wire_container(this, res_wire, res_wire->get_bip_address(managed_shared_memory_.get()));
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
            throw std::runtime_error("cannot find a session with the specified name");
        }
    }

    ~session_wire_container() = default;

    void close() {
        request_wire_.disconnect();
    }

    /**
     * @brief Copy and move constructers are deleted.
     */
    session_wire_container(session_wire_container const&) = delete;
    session_wire_container(session_wire_container&&) = delete;
    session_wire_container& operator = (session_wire_container const&) = delete;
    session_wire_container& operator = (session_wire_container&&) = delete;

    static void remove_shm_entry(std::string_view name) {
        std::string sname(name);
        boost::interprocess::shared_memory_object::remove(sname.c_str());
    }

    // handle request and response
    message_header::index_type search_slot() {
        for(message_header::index_type i = 0; i < slot_size; i++) {
            if (!slot_status_.at(i).test_and_set_in_use()) {
                return i;
            }
        }
        throw std::runtime_error("running out of slot");
    }
    void send(const std::string& req_message, message_header::index_type slot_index) {
        std::unique_lock<std::mutex> lock(mtx_send_);
        request_wire_.write(req_message, slot_index);
    }
    void receive(std::string& res_message, message_header::index_type slot_index) {
        slot& my_slot = slot_status_.at(static_cast<std::size_t>(slot_index));

        while (true) {
            {
                std::unique_lock<std::mutex> lock(mtx_receive_);
                cnd_receive_.wait(lock, [this, slot_index]{ return slot_status_.at(static_cast<std::size_t>(slot_index)).valid() || !using_wire_.load(); });
            }
            if (my_slot.valid()) {
                my_slot.consume(res_message);
                cnd_receive_.notify_all();
                return;
            }
            bool expected = false;
            if (!using_wire_.compare_exchange_weak(expected, true)) {
                continue;
            }

            try {
                auto header_received = response_wire_.await();
                auto index_received = header_received.get_idx();
                if (index_received == slot_index) {
                    res_message.resize(header_received.get_length());
                    response_wire_.read(res_message.data());
                    my_slot.receive_and_consume(header_received.get_type());
                    using_wire_.store(false);
                    cnd_receive_.notify_all();
                    return;
                }
                auto& slot_received = slot_status_.at(static_cast<std::size_t>(index_received));
                std::string& message_received = slot_received.pre_receive(header_received.get_type());
                message_received.resize(header_received.get_length());
                response_wire_.read(message_received.data());
                slot_received.post_receive();
                using_wire_.store(false);
                cnd_receive_.notify_all();
            } catch (std::runtime_error& ex) {
                using_wire_.store(false);
                cnd_receive_.notify_all();
                throw ex;
            }
        }
    }

    // handle result set
    std::unique_ptr<resultset_wires_container> create_resultset_wire() {
        return std::make_unique<resultset_wires_container>(this);
    }

    status_provider& get_status_provider() {
        return *status_provider_;
    }

private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    request_wire_container request_wire_{};
    response_wire_container response_wire_{};
    status_provider* status_provider_{};
    std::array<slot, slot_size> slot_status_{};
    std::mutex mtx_send_{};
    std::mutex mtx_receive_{};
    std::condition_variable cnd_receive_{};
    std::atomic_bool using_wire_{};

    void dispose_resultset_wire(std::unique_ptr<resultset_wires_container>& container) {
        container->set_closed();
        container = nullptr;
    }
};

class connection_container
{
    static constexpr std::size_t request_queue_size = (1UL<<12U);  // 4K bytes (tentative)

public:
    explicit connection_container(std::string_view db_name) : db_name_(db_name) {
        try {
            managed_shared_memory_ = std::make_unique<boost::interprocess::managed_shared_memory>(boost::interprocess::open_only, db_name_.c_str());
            connection_queue_ = managed_shared_memory_->find<connection_queue>(connection_queue::name).first;
        }
        catch(const boost::interprocess::interprocess_exception& ex) {
                std::string msg("cannot find a database with the specified name: ");
                msg += db_name;
                throw std::runtime_error(msg.c_str());
        }
    }

    connection_queue& get_connection_queue() noexcept {
        return *connection_queue_;
    }

    std::string connect() {
        auto& que = get_connection_queue();
        auto rid = que.request_admin();  // connect
        if (auto session_id = que.wait(rid); session_id != tateyama::common::wire::connection_queue::session_id_indicating_error) { // wait
            std::string name{db_name_};
            name += "-";
            name += std::to_string(session_id);
            return name;
        }
        throw std::runtime_error("IPC connection establishment failure");
    }
    
private:
    std::string db_name_;
    std::unique_ptr<boost::interprocess::managed_shared_memory> managed_shared_memory_{};
    connection_queue* connection_queue_;
};

};  // namespace tateyama::common::wire
