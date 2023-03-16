/*
 * Copyright 2018-2021 tsurugi project.
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

#include <jogasaki/api.h>
#include <jogasaki/api/resource/bridge.h>
#include <tateyama/status.h>
#include <tateyama/framework/service.h>

namespace ogawayama::bridge {

class listener;

/**
 * @brief ogawayama service bridge interface
 * @details this is the interface that commonly implemented by each service.
 * This interface is to add in common service registry.
 */
class service : public tateyama::framework::service {
public:
    static constexpr id_type tag = tateyama::framework::service_id_fdw;

    //@brief human readable label of this component
    static constexpr std::string_view component_label = "fdw_service";

    /**
     * @brief create empty object
     */
    service();

    /**
     * @brief destruct the object
     */
    ~service() override;

    service(service const& other) = delete;
    service& operator=(service const& other) = delete;
    service(service&& other) noexcept = delete;
    service& operator=(service&& other) noexcept = delete;

    /**
     * @brief accessor to the service id
     */
    [[nodiscard]] id_type id() const noexcept override;

    /**
     * @brief setup the component
     */
    bool setup(tateyama::framework::environment&) override;

    /**
     * @brief start the component
     */
    bool start(tateyama::framework::environment& env) override;

    /**
     * @brief shutdown the component
     */
    bool shutdown(tateyama::framework::environment&) override;

    /**
     * @brief request interface not used by this service
     */
    bool operator()(
        std::shared_ptr<tateyama::api::server::request>,
        std::shared_ptr<tateyama::api::server::response>) override {
        std::abort();
    }

    /**
     * @see `tateyama::framework::component::label()`
     */
    [[nodiscard]] std::string_view label() const noexcept override;
private:
    std::unique_ptr<listener> listener_; // to use incomplete object, do not add {} after var. name.
    std::thread listener_thread_;
};

}
