/*
 * Copyright 2019-2023 tsurugi project.
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

#include <boost/foreach.hpp>

#include "connectionImpl.h"

namespace ogawayama::stub {

Connection::Impl::Impl(Stub::Impl* manager, std::string_view session_id, std::size_t pgprocno)
    : manager_(manager), session_id_(session_id), wire_(session_id_), transport_(wire_), pgprocno_(pgprocno)
{
    // for old-fashioned link
    std::string name{manager->get_shm_name()};
    name += "-" + std::to_string(pgprocno);
    shm4_connection_ = std::make_unique<ogawayama::common::SharedMemory>(name, ogawayama::common::param::SheredMemoryType::SHARED_MEMORY_CONNECTION, true, true);
    channel_ = std::make_unique<ogawayama::common::ChannelStream>(ogawayama::common::param::channel, shm4_connection_.get(), true);
}

Connection::Impl::~Impl()
{
    transport_.close();
    // for old-fashioned link
    channel_->send_req(ogawayama::common::CommandMessage::Type::DISCONNECT);
    channel_->wait();
}

ErrorCode Connection::Impl::confirm()
{
    ErrorCode reply = channel_->recv_ack();
    if (reply != ErrorCode::OK) {
        return ErrorCode::SERVER_FAILURE;
    }
    return reply;
}

/**
 * @brief begin transaction
 * @param reference to a TransactionPtr
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::begin(TransactionPtr& transaction)
{
    ::jogasaki::proto::sql::request::Begin request{};
    auto response_opt = transport_.send(request);
    if (!response_opt) {
        return ErrorCode::SERVER_FAILURE;
    }
    auto response_begin = response_opt.value();
    if (response_begin.has_success()) {
        transaction = std::make_unique<Transaction>(std::make_unique<Transaction::Impl>(this, transport_, response_begin.success().transaction_handle()));
        return ErrorCode::OK;
    }
    return ErrorCode::SERVER_FAILURE;
}

/**
 * @brief begin transaction
 * @param option the transaction option
 * @param reference to a TransactionPtr
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::begin(const boost::property_tree::ptree& option, TransactionPtr& transaction)
{
    ::jogasaki::proto::sql::request::Begin request{};

    auto* topt = request.mutable_option();
    auto ttype = option.get_optional<std::int64_t>(TRANSACTION_TYPE);
    if (ttype) {
        topt->set_type(static_cast<::jogasaki::proto::sql::request::TransactionType>(ttype.value()));
    }
    auto tprio = option.get_optional<std::int64_t>(TRANSACTION_PRIORITY);
    if (tprio) {
        topt->set_priority(static_cast<::jogasaki::proto::sql::request::TransactionPriority>(tprio.value()));
    }
    auto tlabel = option.get_optional<std::string>(TRANSACTION_LABEL);
    if (tlabel) {
        topt->set_label(tlabel.value());
    }
    BOOST_FOREACH (const auto& wp_node, option.get_child(WRITE_PRESERVE)) {
        auto wp = wp_node.second.get_optional<std::string>(TABLE_NAME);
        if (wp) {
            topt->add_write_preserves()->set_table_name(wp.value());
        }
    }

    auto response_opt = transport_.send(request);
    if (!response_opt) {
        return ErrorCode::SERVER_FAILURE;
    }
    auto response_begin = response_opt.value();
    if (response_begin.has_success()) {
        transaction = std::make_unique<Transaction>(std::make_unique<Transaction::Impl>(this, transport_, response_begin.success().transaction_handle()));
        return ErrorCode::OK;
    }
    return ErrorCode::SERVER_FAILURE;
}

/**
 * @brief prepare statement
 */
ErrorCode Connection::Impl::prepare(std::string_view sql, const pralceholders_type& placeholders, PreparedStatementPtr& prepared)
{
    // FIXME implement
    return ErrorCode::UNSUPPORTED;
}

/**
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::create_table(std::size_t id)
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::CREATE_TABLE, id);
    return channel_->recv_ack();
}

/**
 * @brief relay a drop tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::drop_table(std::size_t id)
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::DROP_TABLE, id);
    return channel_->recv_ack();
}

/**
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::begin_ddl()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::BEGIN_DDL);
    return channel_->recv_ack();
}

/**
 * @brief relay a create tabe message from the frontend to the server
 * @param table id given by the frontend
 * @return error code defined in error_code.h
 */
ErrorCode Connection::Impl::end_ddl()
{
    channel_->send_req(ogawayama::common::CommandMessage::Type::END_DDL);
    return channel_->recv_ack();
}


/**
 * @brief constructor of Connection class
 */
Connection::Connection(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

/**
 * @brief destructor of Connection class
 */
Connection::~Connection() = default;

/**
 * @brief get transaction object, meaning transaction begin
 */
ErrorCode Connection::begin(TransactionPtr &transaction) { return impl_->begin(transaction); }

/**
 * @brief get transaction object, meaning transaction begin
 */
ErrorCode Connection::begin(const boost::property_tree::ptree& option, TransactionPtr &transaction) { return impl_->begin(option, transaction); }

/**
 * @brief prepare statement
 */
ErrorCode Connection::prepare(std::string_view sql, const pralceholders_type& placeholders, PreparedStatementPtr &prepared) { return impl_->prepare(sql, placeholders, prepared); }

/**
 * @brief receive a begin_ddl message from manager
 */
manager::message::Status Connection::receive_begin_ddl(const int64_t mode) const
{
    ErrorCode reply = impl_->begin_ddl();
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a end_ddl message from manager
 */
manager::message::Status Connection::receive_end_ddl() const
{
    ErrorCode reply = impl_->end_ddl();
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a create_table message from manager
 */
manager::message::Status Connection::receive_create_table(const manager::metadata::ObjectIdType object_id) const
{
    ErrorCode reply = impl_->create_table(static_cast<std::size_t>(object_id));
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

/**
 * @brief receive a drop_table message from manager
 */
manager::message::Status Connection::receive_drop_table(const manager::metadata::ObjectIdType object_id) const
{
    ErrorCode reply = impl_->drop_table(static_cast<std::size_t>(object_id));
    return manager::message::Status(reply == ErrorCode::OK ? manager::message::ErrorCode::SUCCESS : manager::message::ErrorCode::FAILURE, static_cast<int>(reply));
}

}  // namespace ogawayama::stub
