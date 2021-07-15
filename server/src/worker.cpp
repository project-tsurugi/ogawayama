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

#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <msgpack.hpp>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <yugawara/storage/configurable_provider.h>
#include<chrono>

#include "worker.h"

namespace ogawayama::server {

DECLARE_string(dbname);

constexpr static int str_length = 1024;
// using Clock = std::chrono::steady_clock;
using Clock = std::chrono::high_resolution_clock;
using Duration = std::chrono::microseconds;

void Worker::run()
{
    request::Request request;
    std::chrono::time_point<Clock> begin;
    std::size_t count = 0;
    std::size_t total = 0;
    
    dummy_string_.resize(str_length);
    for(std::size_t i = 0; i < str_length; i++) {
//        dummy_string_.at(i) = rand() % 255 + 1;
        dummy_string_.at(i) = rand() % 26 + 'a';
    }
    
    while(true) {
    auto h = request_wire_container_.peep(true);
    begin = Clock::now();
    std::size_t length = h.get_length();
    std::string msg;
    msg.resize(length);
    request_wire_container_.read(reinterpret_cast<signed char*>(msg.data()), length);
    if (!request.ParseFromString(msg)) { LOG(ERROR) << "parse error" << std::endl; }
    else {
        VLOG(1) << "s:" << request.session_handle().handle() << "-"
                << "idx:" << h.get_idx()
                << "len:" << length
                << std::endl;
    }

    switch (request.request_case()) {
    case request::Request::RequestCase::kBegin:
        if (!transaction_) {
            transaction_ = db_.create_transaction();
        } else {
            LOG(ERROR) << "already begin a transaction" << std::endl;
            error<protocol::Begin>("transaction has already begun", h.get_idx());
        }
        {
            ::common::Transaction t;
            protocol::Begin b;
            protocol::Response r;

            t.set_handle(++transaction_id_);
            b.set_allocated_transaction_handle(&t);
            r.set_allocated_begin(&b);
            reply(r, h.get_idx());
            r.release_begin();
            b.release_transaction_handle();
        }
        
        VLOG(1) << "begin" << std::endl;
        break;
    case request::Request::RequestCase::kPrepare:
        VLOG(1) << "prepare" << std::endl;
        {
            std::size_t sid = prepared_statements_index_;

            auto pp = request.mutable_prepare();
            auto hvs = pp->mutable_host_variables();
            auto sql = pp->mutable_sql();
            VLOG(1) << "idx:" << h.get_idx() << " : "
                      << *sql
                      << std::endl;
            if (prepared_statements_.size() < (sid + 1)) {
                prepared_statements_.resize(sid + 1);
            }

            ::common::PreparedStatement ps;
            protocol::Prepare p;
            protocol::Response r;

            ps.set_handle(sid);
            *p.mutable_prepared_statement_handle() = ps;
            r.set_allocated_prepare(&p);
            reply(r, h.get_idx());
            r.release_prepare();
            p.release_prepared_statement_handle();

            prepared_statements_index_ = sid + 1;
        }
        break;
    case request::Request::RequestCase::kExecuteStatement:
        VLOG(1) << "execute_statement" << std::endl;
        {
            auto eq = request.mutable_execute_statement();
            VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                      << "idx:" << h.get_idx() << " : "
                      << *(eq->mutable_sql())
                      << std::endl;
//            execute_statement(*(eq->mutable_sql()));
            protocol::Success s;
            protocol::ResultOnly ok;
            protocol::Response r;

            ok.set_allocated_success(&s);
            r.set_allocated_result_only(&ok);
            reply(r, h.get_idx());
            r.release_result_only();
            ok.release_success();
        }
        break;
    case request::Request::RequestCase::kExecuteQuery:
        VLOG(1) << "execute_query" << std::endl;
        {
            std::size_t rsid = search_resultset();

            auto eq = request.mutable_execute_query();
            VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                    << "idx:" << h.get_idx() << " : "
                    << *(eq->mutable_sql())
                    << "rsid:" << rsid
                    << std::endl;

            if (cursors_.size() < (rsid + 1)) {
                cursors_.resize(rsid + 1);
                cursors_.at(rsid).wire_name_ = std::string("resultset-");
                cursors_.at(rsid).wire_name_ += std::to_string(rsid);
                cursors_.at(rsid).resultset_wire_container_ = wire_->create_resultset_wire(cursors_.at(rsid).wire_name_);
            }

            protocol::ExecuteQuery e;
            protocol::Response r;

            e.set_name(cursors_.at(rsid).wire_name_);
            r.set_allocated_execute_query(&e);
            reply(r, h.get_idx());
            r.release_execute_query();

            next(rsid);
        }
        break;
    case request::Request::RequestCase::kExecutePreparedStatement:
        VLOG(1) << "execute_prepared_statement" << std::endl;
        {
            auto pq = request.mutable_execute_prepared_statement();
            auto ph = pq->mutable_prepared_statement_handle();
            auto sid = ph->handle();
            VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle() << "-"
                      << "idx:" << h.get_idx() << " : "
                      << "sid:" << sid
                      << std::endl;

            protocol::Success s;
            protocol::ResultOnly ok;
            protocol::Response r;

            ok.set_allocated_success(&s);
            r.set_allocated_result_only(&ok);
            reply(r, h.get_idx());
            r.release_result_only();
            ok.release_success();
        }
        break;
    case request::Request::RequestCase::kExecutePreparedQuery:
        VLOG(1) << "execute_prepared_query" << std::endl;
        {
            std::size_t rsid = search_resultset();

            auto pq = request.mutable_execute_prepared_query();
            auto ph = pq->mutable_prepared_statement_handle();
            auto sid = ph->handle();
            VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle() << "-"
                    << "idx:" << h.get_idx() << " : "
                    << "psid:" << sid << " : "
                    << "rsid:" << rsid
                    << std::endl;

            if (cursors_.size() < (rsid + 1)) {
                cursors_.resize(rsid + 1);
                cursors_.at(rsid).wire_name_ = std::string("resultset-");
                cursors_.at(rsid).wire_name_ += std::to_string(rsid);
                cursors_.at(rsid).resultset_wire_container_ = wire_->create_resultset_wire(cursors_.at(rsid).wire_name_);
            }

            protocol::ExecuteQuery e;
            protocol::Response r;

            e.set_name(cursors_.at(rsid).wire_name_);
            r.set_allocated_execute_query(&e);
            reply(r, h.get_idx());
            r.release_execute_query();

            next(rsid);
        }
        break;
    case request::Request::RequestCase::kCommit:
        VLOG(1) << "commit" << std::endl;
        {
            if (transaction_) {
                transaction_->commit();
                transaction_ = nullptr;
            } else {
                LOG(ERROR) << "transaction has not begun" << std::endl;
            }

            auto eq = request.mutable_commit();
            VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << "-"
                      << "idx:" << h.get_idx()
                      << std::endl;

            protocol::Success s;
            protocol::ResultOnly ok;
            protocol::Response r;

            ok.set_allocated_success(&s);
            r.set_allocated_result_only(&ok);
            reply(r, h.get_idx());
            r.release_result_only();
            ok.release_success();
        }
        break;
    case request::Request::RequestCase::kRollback:
        VLOG(1) << "rollback" << std::endl;
        break;
    case request::Request::RequestCase::kDisposePreparedStatement:
        VLOG(1) << "dispose_prepared_statement" << std::endl;
        {
            auto dp = request.mutable_dispose_prepared_statement();
            auto ph = dp->mutable_prepared_statement_handle();
            auto sid = ph->handle();

            VLOG(1) << "idx:" << h.get_idx() << " : "
                    << "ps:" << sid
                    << std::endl;

            if(prepared_statements_.size() > sid) {
                prepared_statements_.at(sid) = nullptr;

                protocol::Success s;
                protocol::ResultOnly ok;
                protocol::Response r;

                ok.set_allocated_success(&s);
                r.set_allocated_result_only(&ok);
                reply(r, h.get_idx());
                r.release_result_only();
                ok.release_success();
            } else {
                error<protocol::ResultOnly>("index is larger than the number of prepred statment registerd", h.get_idx());
            }
        }
        break;
    case request::Request::RequestCase::kDisconnect:
        std::cout << "result: " << count << ":" << total << ":" << total / count << std::endl;
        VLOG(1) << "disconnect" << std::endl;
        {
            protocol::Success s;
            protocol::ResultOnly ok;
            protocol::Response r;

            ok.set_allocated_success(&s);
            r.set_allocated_result_only(&ok);
            reply(r, h.get_idx());
            r.release_result_only();
            ok.release_success();
        }
        return;
    case request::Request::RequestCase::REQUEST_NOT_SET:
        VLOG(1) << "not used" << std::endl;
        break;
    default:
        LOG(ERROR) << "????" << std::endl;
        break;
    }
    total += std::chrono::duration_cast<Duration>(Clock::now() - begin).count();
    count++;
    }
}

void Worker::next(std::size_t rsid)
{
    auto& cursor = cursors_.at(rsid);

    schema::RecordMeta meta;
    schema::RecordMeta_Column column;
    column.set_type(::common::DataType::STRING);
    column.set_nullable(false);
    *meta.add_columns() = column;

    std::string output;
    if (!meta.SerializeToString(&output)) { std::abort(); }
    cursors_.at(rsid).resultset_wire_container_->write(reinterpret_cast<const signed char*>(output.data()),
                                                       tsubakuro::common::wire::length_header(output.size()));

    tsubakuro::common::wire::server_wire_container::resultset_wire_container& wire = *cursor.resultset_wire_container_;
    
    msgpack::pack(wire, dummy_string_);
    wire.set_eor();
}


}  // ogawayama::server
