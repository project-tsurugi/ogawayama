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
#include <exception>
#include <string>

#include <glog/logging.h>

#include "ipc_response.h"

namespace tsubakuro::common::wire {


// class ipc_response
tateyama::status ipc_response::body(std::string_view body) {
    VLOG(1) << __func__ << std::endl;

    if(body.length() > response_box::response::max_response_message_length) {
        return tateyama::status::unknown;
    }
    memcpy(response_box_.get_buffer(body.length()), body.data(), body.length());
    return tateyama::status::ok;
}

tateyama::status ipc_response::complete() {
    VLOG(1) << __func__ << std::endl;

    ipc_request_.dispose();
    if (response_code_ == tateyama::api::endpoint::response_code::success) {
        response_box_.flush();
    }
    return tateyama::status::ok;
}

void ipc_response::code(tateyama::api::endpoint::response_code code) {
    VLOG(1) << __func__ << std::endl;

    response_code_ = code;
}

void ipc_response::message(std::string_view msg) {
    message_ = msg;
}

tateyama::status ipc_response::acquire_channel(std::string_view name, tateyama::api::endpoint::data_channel*& ch) {
    data_channel_ = std::make_unique<ipc_data_channel>(server_wire_.create_resultset_wires(name));
    if (data_channel_.get() != nullptr) {
        ch = data_channel_.get();
        response_box_.flush();
        return tateyama::status::ok;
    }
    return tateyama::status::unknown;
}

tateyama::status ipc_response::release_channel(tateyama::api::endpoint::data_channel& ch) {
    if (data_channel_.get() == static_cast<ipc_data_channel*>(&ch)) {
        data_channel_ = nullptr;
        return tateyama::status::ok;
    }
    return tateyama::status::unknown;
}

// class ipc_data_channel
tateyama::status ipc_data_channel::acquire(tateyama::api::endpoint::writer*& wrt) {
    if (auto ipc_wrt = std::make_unique<ipc_writer>(data_channel_->acquire()); ipc_wrt.get() != nullptr) {
        wrt = ipc_wrt.get();
        data_writers_.emplace(std::move(ipc_wrt));
        return tateyama::status::ok;
    }
    return tateyama::status::unknown;
}

tateyama::status ipc_data_channel::release(tateyama::api::endpoint::writer& wrt) {
    if (auto itr = data_writers_.find(static_cast<ipc_writer*>(&wrt)); itr != data_writers_.end()) {
        data_writers_.erase(itr);
        return tateyama::status::ok;
    }
    return tateyama::status::unknown;
}

// class writer
tateyama::status ipc_writer::write(char const* data, std::size_t length) {
    resultset_wire_->write(data, length);
    return tateyama::status::ok;
}

tateyama::status ipc_writer::commit() {
    resultset_wire_->commit();
    return tateyama::status::ok;
}

}  // tsubakuro::common::wire
