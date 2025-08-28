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

#include <future>
#include <thread>

#include <boost/archive/basic_archive.hpp> // need for ubuntu 20.04
#include <boost/property_tree/ptree_serialization.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>

#include <jogasaki/api.h>
#include <ogawayama/stub/metadata.h>
#include <ogawayama/stub/error_code.h>
#include <manager/metadata/tables.h>
#include <manager/metadata/metadata.h>
#include <manager/metadata/error_code.h>
#include <manager/metadata/datatypes.h>

using ERROR_CODE = ogawayama::stub::ErrorCode;

namespace ogawayama::bridge {

class Worker {
public:
    Worker() = default;
    ~Worker();

    Worker(Worker const& other) = delete;
    Worker& operator=(Worker const& other) = delete;
    Worker(Worker&& other) noexcept = delete;
    Worker& operator=(Worker&& other) noexcept = delete;

    ERROR_CODE begin_ddl(jogasaki::api::database&);
    ERROR_CODE end_ddl(jogasaki::api::database&);
    static ERROR_CODE deploy_table(jogasaki::api::database&, std::string_view);
    static ERROR_CODE do_deploy_table(jogasaki::api::database&, boost::archive::binary_iarchive& ia);
    static ERROR_CODE withdraw_table(jogasaki::api::database&, std::string_view);
    static ERROR_CODE do_withdraw_table(jogasaki::api::database&, boost::archive::binary_iarchive& ia);
    static ERROR_CODE do_deploy_index(jogasaki::api::database&, boost::archive::binary_iarchive& ia);
    static ERROR_CODE do_withdraw_index(jogasaki::api::database&, boost::archive::binary_iarchive& ia);

 private:
    jogasaki::api::transaction_handle transaction_handle_{};
};

}  // ogawayama::bridge
