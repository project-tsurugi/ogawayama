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
#include "ipc_request.h"

namespace tsubakuro::common::wire {

server_wire_container&
ipc_request::get_server_wire_container() {
    return server_wire_;
}

std::string_view
ipc_request::payload() const {
    auto wire = server_wire_.get_request_wire();
    if (auto address = wire.payload(length_); address != nullptr) {
        return std::string_view(static_cast<const char*>(address), length_);
    }
//    payload_.resize(length_);
//    wire.read(payload_.data(), length_);
    return payload_;
}

void
ipc_request::dispose() {
    server_wire_.get_request_wire().dispose(read_point);
}

}  // tsubakuro::common::wire