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

#include "umikongo/api.h"
#include "ogawayama/common/channel_stream.h"

namespace ogawayama::server {

int backend_main(int, char **);
void worker_main(umikongo::Database *, ogawayama::common::SharedMemory *, int);

template <class T>
class DBCloser final { //NOLINT
public:
    DBCloser() = delete; //NOLINT
    DBCloser(const DBCloser& other) = delete; //NOLINT
    DBCloser(DBCloser&& other) = delete; //NOLINT
    DBCloser& operator=(const DBCloser& other) = delete; //NOLINT
    DBCloser& operator=(DBCloser&& other) = delete;
    ~DBCloser() {
        t_->close();
    }
    explicit DBCloser(T& t) : t_(t) {} //NOLINT
private:
    T& t_;
};

}  // ogawayama::server
