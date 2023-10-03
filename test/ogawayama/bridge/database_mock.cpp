/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include "database_mock.h"

namespace ogawayama::testing {

database_mock::database_mock() {
}

[[nodiscard]] jogasaki::status database_mock::start() {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::stop() {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::prepare(
    std::string_view,
    jogasaki::api::statement_handle&
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::prepare(
    std::string_view,
    std::unordered_map<std::string, jogasaki::api::field_type_kind> const&,
    jogasaki::api::statement_handle&
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::create_executable(
    std::string_view,
    std::unique_ptr<jogasaki::api::executable_statement>&
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::resolve(
    jogasaki::api::statement_handle,
    maybe_shared_ptr<jogasaki::api::parameter_set const>,
    std::unique_ptr<jogasaki::api::executable_statement>&
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::destroy_statement(
    jogasaki::api::statement_handle
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::destroy_transaction(
    jogasaki::api::transaction_handle
) {
    return jogasaki::status::ok;
}

[[nodiscard]] jogasaki::status database_mock::explain(jogasaki::api::executable_statement const&, std::ostream&) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::dump(std::ostream&, std::string_view, std::size_t) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::load(std::istream&, std::string_view, std::size_t) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::do_create_transaction(transaction_handle&, transaction_option const&) {
    return jogasaki::status::ok;
}

jogasaki::scheduler::job_context::job_id_type database_mock::do_create_transaction_async(
    create_transaction_callback,
    transaction_option const&
) {
    return 0;
}

std::shared_ptr<class jogasaki::configuration>& database_mock::config() noexcept {
    return cfg_;
}

void database_mock::print_diagnostic(std::ostream&) {
}

std::string database_mock::diagnostic_string() {
    return "";
}

jogasaki::status database_mock::do_create_table(
    std::shared_ptr<yugawara::storage::table> table,
    std::string_view
) {
    table_ = std::move(table);
    return jogasaki::status::ok;
}

std::shared_ptr<yugawara::storage::table const> database_mock::do_find_table(
    std::string_view,
    std::string_view
) {
    return nullptr;
}

jogasaki::status database_mock::do_drop_table(
    std::string_view,
    std::string_view
) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::do_create_index(
    std::shared_ptr<yugawara::storage::index> index,
    std::string_view
) {
    indexes_.emplace_back(std::move(index));
    return jogasaki::status::ok;
}

std::shared_ptr<yugawara::storage::index const> database_mock::do_find_index(
    std::string_view,
    std::string_view
) {
    return nullptr;
}

jogasaki::status database_mock::do_drop_index(
    std::string_view,
    std::string_view
) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::do_create_sequence(
    std::shared_ptr<yugawara::storage::sequence>,
    std::string_view
) {
    return jogasaki::status::ok;
}

std::shared_ptr<yugawara::storage::sequence const> database_mock::do_find_sequence(
    std::string_view,
    std::string_view
) {
    return nullptr;
}

jogasaki::status database_mock::do_drop_sequence(
    std::string_view,
    std::string_view
) {
    return jogasaki::status::ok;
}

jogasaki::status database_mock::list_tables(std::vector<std::string>&) {
    return jogasaki::status::ok;
}

};
