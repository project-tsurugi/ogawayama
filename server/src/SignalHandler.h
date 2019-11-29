/*
 * Copyright 2018-2019 tsurugi project.
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

#include <chrono>
#include <thread>
#include <future>
#include "glog/logging.h"
#include "boost/thread.hpp"
#include "boost/thread/future.hpp"
#include "boost/asio.hpp"

namespace ogawayama::server {

/**
 * @brief logging thread for debugging
 */
class SignalHandler {
public:
    ~SignalHandler() = default;
    SignalHandler(SignalHandler const& other) = delete;
    SignalHandler(SignalHandler&& other) = delete;
    SignalHandler& operator=(SignalHandler const& other) = delete;
    SignalHandler& operator=(SignalHandler&& other) = delete;
    SignalHandler(std::function<void(void)> callback) {
        thread_ = std::make_unique<boost::thread>([this, callback]() {
            boost::asio::signal_set signals(this->io_service_, SIGINT);
            signals.async_wait([callback](const boost::system::error_code& error, int signal_number) {
                if (!error && signal_number == SIGINT) {
                    callback();
                }
            });
            this->io_service_.run();
        });
    }
    void shutdown() {
        this->io_service_.stop();
        thread_->join();
    }
private:
    std::unique_ptr<boost::thread> thread_{};
    boost::asio::io_service io_service_;
};

} // namespace
