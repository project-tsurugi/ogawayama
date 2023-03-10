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
#pragma once

#include <string>
#include <thread>
#include <stdexcept>

#include <glog/logging.h>
#include <ogawayama/logging.h>

#include <ogawayama/stub/api.h>
#include <ogawayama/common/channel_stream.h>

namespace ogawayama::testing {

class channel_stream {
    friend class server;

public:
    class worker {
      public:
        worker(std::string&, std::string_view shm_name, std::size_t id) {
            std::string name{shm_name};
            name += "-" + std::to_string(id);
            try {
                shm4_connection_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_CONNECTION);
                channel_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::channel, shm4_connection_.get());
                channel_->send_ack(ERROR_CODE::OK);
            } catch (std::exception &ex) {
                LOG(ERROR) << ex.what();
            }
            thread_ = std::thread(std::ref(*this));
        }
        ~worker() {
            if (thread_.joinable()) {
                thread_.join();
            }
        }
        void operator()() {
            while(true) {
                ogawayama::common::CommandMessage::Type type;
                std::size_t ivalue{};
                std::string_view string;
                try {
                    if (auto rc = channel_->recv_req(type, ivalue, string); rc != ERROR_CODE::OK) {
                        LOG(WARNING) << "error (" << error_name(rc) << ") occured in command receive, exiting";
                        return;
                    }
                    VLOG(log_debug) << "--> " << ogawayama::common::type_name(type) << " : ivalue = " << ivalue << " : string = " << " \"" << string << "\"";
                } catch (std::exception &ex) {
                    LOG(WARNING) << "exception in command receive, exiting";
                    return;
                }

                switch (type) {
                case ogawayama::common::CommandMessage::Type::DISCONNECT:
                    channel_->bye_and_notify();
                    VLOG(log_debug) << "<-- bye_and_notify()";
                    return;
                default:
                    LOG(ERROR) << "recieved an illegal command message";
                    return;
                }
            }
        }
        std::string_view payload() {
            return payload_;
        }
    private:
        std::thread thread_;
        std::unique_ptr<ogawayama::common::SharedMemory> shm4_connection_;
        std::unique_ptr<ogawayama::common::ChannelStream> channel_;
        std::string payload_;
    };


    channel_stream(std::string name)
        : name_(name),
          shared_memory_(std::make_unique<ogawayama::common::SharedMemory>(name_, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_SERVER_CHANNEL, true, true)),
          bridge_ch_(std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory_.get(), true, false)),
          thread_(std::thread(std::ref(*this))) {
    }
    ~channel_stream() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string_view get_shared_memory_name() {
        return shared_memory_->get_name();
    }
    void operator()() {
        while(true) {
            ogawayama::common::CommandMessage::Type type;
            std::size_t index{};
            std::string_view string;
            try {
                auto rv = bridge_ch_->recv_req(type, index, string);
                if (rv != ERROR_CODE::OK) {
                    if (rv != ERROR_CODE::TIMEOUT) {
                        LOG(ERROR) << ogawayama::stub::error_name(rv) << std::endl;
                    }
                    continue;
                }
            } catch (std::exception &ex) {
                LOG(ERROR) << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
                break;
            }

            switch (type) {
                case ogawayama::common::CommandMessage::Type::CONNECT:
                    try {
                        worker_ = std::make_unique<worker>(name_, shared_memory_->get_name(), index);
                    } catch (std::exception &ex) {
                        LOG(ERROR) << ex.what() << std::endl;
                        return;
                    }
                    break;
                case ogawayama::common::CommandMessage::Type::TERMINATE:
                    bridge_ch_->notify();
                    bridge_ch_->lock();
                    bridge_ch_->unlock();
                    return;
                default:
                    LOG(ERROR) << "unsurpported message" << std::endl;
                    bridge_ch_->notify();
                    return;
            }
            bridge_ch_->notify();
        }
    }
    void terminate() {
        bridge_ch_->lock();
        bridge_ch_->send_req(ogawayama::common::CommandMessage::Type::TERMINATE);
        bridge_ch_->wait();
        bridge_ch_->unlock();
    }
private:
    std::string name_;
    std::unique_ptr<ogawayama::common::SharedMemory> shared_memory_;
    std::unique_ptr<ogawayama::common::ChannelStream> bridge_ch_;
    std::thread thread_;

    std::unique_ptr<worker> worker_{};
};

}  // namespace ogawayama::testing
