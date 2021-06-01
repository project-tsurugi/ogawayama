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
#include "utils.h"

#include <vector>
#include <fstream>
#include <string>

#include "gflags/gflags.h"

#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"

namespace ogawayama::server {

    DECLARE_int32(read_batch_size);   //NOLINT
    DECLARE_int32(write_batch_size);  //NOLINT

    boost::filesystem::path prepare(std::string location) {
        boost::filesystem::path dir(location);
        dir = dir / "dump";
        if (!boost::filesystem::exists(dir)) {
            if (!boost::filesystem::create_directories(dir)) {
                throw std::ios_base::failure("Failed to create directory.");
            }
        }
        return dir;
    }

    void
    dump(jogasaki::api::database& db, std::string &location, std::string &table)
    {
        boost::filesystem::path dir = prepare(location);
        std::ofstream ofs((dir / (table+".tbldmp")).c_str());
        if (ofs.fail()) {
            throw std::ios_base::failure("Failed to open file.");
        }
        db.dump(ofs, table, FLAGS_read_batch_size);
    }

    void
    load(jogasaki::api::database& db, std::string &location, std::string &table)
    {
        boost::filesystem::path dir = prepare(location);
        std::ifstream ifs((dir / (table+".tbldmp")).c_str());
        if (ifs.fail()) {
            throw std::ios_base::failure("Failed to open file.");
        }
        db.load(ifs, table, FLAGS_write_batch_size);
    }

}  // ogawayama::server
