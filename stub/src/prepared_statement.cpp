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

#include "prepared_statementImpl.h"

namespace ogawayama::stub {

/**
 * @brief constructor of PreparedStatement class
 */
PreparedStatement::PreparedStatement(Connection *connection, std::size_t sid) : manager_(connection) {
    impl_ = std::make_unique<PreparedStatement::Impl>(this, sid, connection->get_impl()->get_parameters());
}

/**
 * @brief destructor of PreparedStatement class
 */
PreparedStatement::~PreparedStatement() = default;

void PreparedStatement::set_parameter(std::int16_t param) {
    get_impl()->set_parameter<std::int16_t>(param);
}
void PreparedStatement::set_parameter(std::int32_t param) {
    get_impl()->set_parameter<std::int32_t>(param);
}
void PreparedStatement::set_parameter(std::int64_t param) {
    get_impl()->set_parameter<std::int64_t>(param);
}
void PreparedStatement::set_parameter(float param) {
    get_impl()->set_parameter<float>(param);
}
void PreparedStatement::set_parameter(double param) {
    get_impl()->set_parameter<double>(param);
}
void PreparedStatement::set_parameter(std::string_view param) {
    get_impl()->set_parameter<std::string_view>(param);
}
void PreparedStatement::set_parameter() {
    get_impl()->set_parameter<std::monostate>(std::monostate());
}

}  // namespace ogawayama::stub
