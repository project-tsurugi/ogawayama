/*
 * Copyright 2019-2022 tsurugi project.
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
#include <memory>
#include <string>
#include <exception>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <tateyama/api/registry.h>
#include <tateyama/api/environment.h>
#include <tateyama/api/configuration.h>

#include <ogawayama/bridge/provider.h>
#include "worker.h"

namespace ogawayama::bridge {

DECLARE_bool(remove_shm);

/**
 * @brief fe endpoint provider
 * @details
 */
class fe_provider : public ogawayama::bridge::api::provider {
private:
    class listener {
    public:
        listener() = delete;

        listener(tateyama::api::environment& env, jogasaki::api::database* db) : db_(db) {
            auto endpoint_config = env.configuration()->get_section("ogawayama"); 
            if (endpoint_config == nullptr) {
                LOG(ERROR) << "cannot find ogawayama section in the configuration";
                exit(1);
            }
            if (!endpoint_config->get<>("name", name_)) {
                LOG(ERROR) << "cannot find name at the section in the configuration";
                exit(1);
            }
            std::size_t threads{};
            if (!endpoint_config->get<>("threads", threads)) {
                LOG(ERROR) << "cannot find thread_pool_size at the section in the configuration";
                exit(1);
            }

            // communication channel
            try {
                shared_memory_ = std::make_unique<ogawayama::common::SharedMemory>(name_, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_SERVER_CHANNEL, true, FLAGS_remove_shm);
                bridge_ch_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::server, shared_memory_.get(), true, false);
            } catch (const std::exception& ex) {
                LOG(ERROR) << ex.what() << std::endl;
            }

            // worker objects
            workers_.reserve(threads);
        }

        void operator()() {
            while(true) {
                ogawayama::common::CommandMessage::Type type;
                std::size_t index;
                std::string_view string;
                try {
                    auto rv = bridge_ch_->recv_req(type, index, string);
                    if (rv != ERROR_CODE::OK) {
                        if (rv != ERROR_CODE::TIMEOUT) {
                            LOG(ERROR) << __func__ << " " << __LINE__ <<  " " << ogawayama::stub::error_name(rv) << std::endl;
                        }
                        continue;
                    }
                } catch (std::exception &ex) {
                    LOG(ERROR) << __func__ << " " << __LINE__ << ": exiting \"" << ex.what() << "\"" << std::endl;
                    goto finish;
                }

                switch (type) {
                case ogawayama::common::CommandMessage::Type::CONNECT:
                    if (workers_.size() < (index + 1)) {
                        workers_.resize(index + 1);
                    }
                    try {
                        std::unique_ptr<Worker> &worker = workers_.at(index);
                        worker = std::make_unique<Worker>(*db_, name_, shared_memory_->get_name(), index);
                        worker->task_ = std::packaged_task<void()>([&]{worker->run();});
                        worker->future_ = worker->task_.get_future();
                        worker->thread_ = std::thread(std::move(worker->task_));
                    } catch (std::exception &ex) {
                        LOG(ERROR) << ex.what() << std::endl;
                        goto finish;
                    }
                    break;
                case ogawayama::common::CommandMessage::Type::TERMINATE:
                    bridge_ch_->notify();
                    bridge_ch_->lock();
                    bridge_ch_->unlock();
                    goto finish;
                default:
                    LOG(ERROR) << "unsurpported message" << std::endl;
                    bridge_ch_->notify();
                    goto finish;
                }
                bridge_ch_->notify();
            }

          finish:
            return;
        }

        void terminate() {
            bridge_ch_->lock();
            bridge_ch_->send_req(ogawayama::common::CommandMessage::Type::TERMINATE);
            bridge_ch_->wait();
            bridge_ch_->unlock();
        }

    private:
        jogasaki::api::database* db_;
        std::string name_;
        std::unique_ptr<ogawayama::common::SharedMemory> shared_memory_;
        std::unique_ptr<ogawayama::common::ChannelStream> bridge_ch_;
        std::vector<std::unique_ptr<Worker>> workers_;
    };

    public:
    tateyama::status initialize(tateyama::api::environment& env, jogasaki::api::database* db, [[maybe_unused]] void* context) override {
        // create listener object
        listener_ = std::make_unique<listener>(env, db);

        return tateyama::status::ok;
    }

    tateyama::status start() override {
        listener_thread_ = std::thread(std::ref(*listener_));
        return tateyama::status::ok;
    }

    tateyama::status shutdown() override {
        listener_->terminate();
        listener_thread_.join();
        return tateyama::status::ok;
    }

    static std::shared_ptr<fe_provider> create() {
        return std::make_shared<fe_provider>();
    }

private:
    std::unique_ptr<listener> listener_;
    std::thread listener_thread_;
};  

}  // ogawayama::bridge
