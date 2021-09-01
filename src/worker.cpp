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

//#include <vector>
//#include <boost/foreach.hpp>

//#include <gflags/gflags.h>
//#include <glog/logging.h>

#include "worker.h"

namespace ogawayama::server {

void Worker::run()
{
    while(true) {
        garbage_collector_.do_collect();
        auto h = request_wire_container_.peep(true);
        auto request = std::make_shared<tsubakuro::common::wire::ipc_request>(*wire_, h);
        auto response = std::make_shared<tsubakuro::common::wire::ipc_response>(*request, h.get_idx(), garbage_collector_);
        service_(static_cast<std::shared_ptr<tateyama::api::endpoint::request const>>(request),
                 static_cast<std::shared_ptr<tateyama::api::endpoint::response>>(response));
        if (response->is_session_closed()) { break; }
    }
}

}  // ogawayama::server
