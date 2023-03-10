/*
 * Copyright 2018-2023 tsurugi project.
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

#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/database.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/status.h>

namespace jogasaki::scheduler {
class task_scheduler;
class job_context {
public:
    using job_id_type = std::size_t;
};
}

namespace ogawayama::testing {

using takatori::util::maybe_shared_ptr;

/**
 * @brief database interface to start/stop the services and initiate transaction requests
 */
class database_mock : public jogasaki::api::database {
public:
    using status = jogasaki::status;
    using transaction_handle = jogasaki::api::transaction_handle;
    using transaction_option = jogasaki::api::transaction_option;

    using create_transaction_callback = jogasaki::api::database::create_transaction_callback;

    database_mock();

    [[nodiscard]] jogasaki::status start() override;

    [[nodiscard]] jogasaki::status stop() override;

    [[nodiscard]] jogasaki::status prepare(
        std::string_view sql,
        jogasaki::api::statement_handle& statement
    ) override;

    [[nodiscard]] jogasaki::status prepare(
        std::string_view sql,
        std::unordered_map<std::string, jogasaki::api::field_type_kind> const& variables,
        jogasaki::api::statement_handle& statement
    ) override;

    [[nodiscard]] jogasaki::status create_executable(
        std::string_view sql,
        std::unique_ptr<jogasaki::api::executable_statement>& statement
    ) override;

    [[nodiscard]] jogasaki::status resolve(
        jogasaki::api::statement_handle prepared,
        maybe_shared_ptr<jogasaki::api::parameter_set const> parameters,
        std::unique_ptr<jogasaki::api::executable_statement>& statement
    ) override;

    [[nodiscard]] jogasaki::status destroy_statement(
        jogasaki::api::statement_handle prepared
    ) override;

    [[nodiscard]] jogasaki::status destroy_transaction(
        jogasaki::api::transaction_handle handle
    ) override;

    [[nodiscard]] jogasaki::status explain(jogasaki::api::executable_statement const& executable, std::ostream& out) override;

    jogasaki::status dump(std::ostream& output, std::string_view index_name, std::size_t batch_size) override;

    jogasaki::status load(std::istream& input, std::string_view index_name, std::size_t batch_size) override;

    jogasaki::status do_create_transaction(transaction_handle& handle, transaction_option const& option) override;

    jogasaki::scheduler::job_context::job_id_type do_create_transaction_async(
        create_transaction_callback on_completion,
        transaction_option const& option
    ) override;

    std::shared_ptr<class jogasaki::configuration>& config() noexcept override;

    [[nodiscard]] std::shared_ptr<jogasaki::diagnostics> fetch_diagnostics() noexcept override;

    void print_diagnostic(std::ostream& os) override;

    std::string diagnostic_string() override;


    // for test
    std::shared_ptr<yugawara::storage::table>& get_table() {
        return table_;
    }
    std::vector<std::shared_ptr<yugawara::storage::index>>& get_indexes() {
        return indexes_;
    }

protected:
    jogasaki::status do_create_table(
        std::shared_ptr<yugawara::storage::table> table,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::table const> do_find_table(
        std::string_view name,
        std::string_view schema
    ) override;

    jogasaki::status do_drop_table(
        std::string_view name,
        std::string_view schema
    ) override;

    jogasaki::status do_create_index(
        std::shared_ptr<yugawara::storage::index> index,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::index const> do_find_index(
        std::string_view name,
        std::string_view schema
    ) override;

    jogasaki::status do_drop_index(
        std::string_view name,
        std::string_view schema
    ) override;

    jogasaki::status do_create_sequence(
        std::shared_ptr<yugawara::storage::sequence> sequence,
        std::string_view schema
    ) override;

    std::shared_ptr<yugawara::storage::sequence const> do_find_sequence(
        std::string_view name,
        std::string_view schema
    ) override;

    jogasaki::status do_drop_sequence(
        std::string_view name,
        std::string_view schema
    ) override;

private:
    std::shared_ptr<class jogasaki::configuration> cfg_{};

    std::shared_ptr<yugawara::storage::table> table_{};
    std::vector<std::shared_ptr<yugawara::storage::index>> indexes_{};
};

}
