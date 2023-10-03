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

#include <iostream>
#include <thread>
#include <vector>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <iomanip>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "boost/filesystem.hpp"
#ifdef ENABLE_GOOGLE_PERFTOOLS
#include "gperftools/profiler.h"
#endif

#include "ogawayama/stub/api.h"
#include "tpcc_common.h"
#include "tpcc_transaction.h"
#include "tpcc_scale.h"

namespace ogawayama {
namespace tpcc {

static volatile bool begin, done;

DEFINE_string(dbname, ogawayama::common::param::SHARED_MEMORY_NAME, "database name");  // NOLINT

DEFINE_int32(duration, 10, "Duration of benchmark in seconds.");  //NOLINT
DEFINE_uint32(warehouse, 1, "Database size (TPC-C scale factor).");  //NOLINT
DEFINE_bool(pause_before_exec, false, "Pause 10 seconds before benchmark execution");  //NOLINT
DEFINE_bool(ap_profiler, false, "Application profiler");  //NOLINT
DEFINE_int32(threads, 1, "Number of client threads");  //NOLINT

#ifdef ENABLE_GOOGLE_PERFTOOLS
DEFINE_string (proffile, "", "Performance measurement result file.");
#endif

class tpcc_result {
public:
    tpcc_result() = default;
    void print(int d) {
        std::cout.imbue(std::locale(""));
        std::cout << "neworder:\t" << std::setw(14) << neworderS_ << "\t" << neworderA_ << std::endl;
        std::cout << "payment:\t" << std::setw(14) << payment_ << std::endl;
        std::cout << "orderstatus:\t" << std::setw(14) << orderstatus_ << std::endl;
        std::cout << "delivery:\t" << std::setw(14) << delivery_ << std::endl;
        std::cout << "stocklevel:\t" << std::setw(14) << stocklevel_ << std::endl;
        std::cout << "abort:\t" << std::setw(14) << abort_ << std::endl;
        std::cout << "##NoTPM=" << double_to_string(static_cast<double>((neworderS_ + neworderA_) * 60) / static_cast<double>(d)) << std::endl;
    }
    tpcc_result& operator+=(tpcc_result &rhs) {
        neworderS_   += rhs.neworderS_;
        neworderA_   += rhs.neworderA_;
        payment_     += rhs.payment_;
        orderstatus_ += rhs.orderstatus_;
        delivery_    += rhs.delivery_;
        stocklevel_  += rhs.stocklevel_;
        abort_       += rhs.abort_;
        return *this;
    }
    int neworderS_ {};
    int neworderA_ {};
    int payment_ {};
    int orderstatus_ {};
    int delivery_ {};
    int stocklevel_ {};
    int abort_ {};
};

void tpcc_client(tpcc_result *result, int id, tpcc_profiler *profiler)
{
    auto randomGenerator = std::make_unique<randomGeneratorClass>();
    const std::uint16_t warehouse_low = 1;
    std::uint16_t warehouse_high = warehouses;

    StubPtr stub;
    ConnectionPtr connection;

    if(make_stub(stub) != ERROR_CODE::OK) {
        elog(ERROR, "failed to make_stub");
    }
    if(stub->get_connection(connection, id) != ERROR_CODE::OK) {
        elog(ERROR, "failed to get_connection");
    }

    auto new_order_prepared_statements = std::make_unique<prepared_statements>();
    new_order_prepared_statements->prepare(connection.get(), new_order_statements);
    auto payment_prepared_statements = std::make_unique<prepared_statements>();
    payment_prepared_statements->prepare(connection.get(), payment_statements);
    auto order_status_prepared_statements = std::make_unique<prepared_statements>();
    order_status_prepared_statements->prepare(connection.get(), order_status_statements);
    auto delivery_prepared_statements = std::make_unique<prepared_statements>();
    delivery_prepared_statements->prepare(connection.get(), delivery_statements);
    auto stocklevel_prepared_statements = std::make_unique<prepared_statements>();
    stocklevel_prepared_statements->prepare(connection.get(), stocklevel_statements);

    while(!begin);
    do {
        int transaction_type = randomGenerator->uniformWithin(1, 100);

        profiler->mark_enter();
        try {
            if (transaction_type <= kXctNewOrderPercent) {
                profiler->mark_neworder_begin();
                int rc = transaction_neworder(connection.get(), new_order_prepared_statements.get(), randomGenerator.get(), warehouse_low, warehouse_high, profiler);
                if (rc == 0) {
                    result->neworderS_++;
                } else if (rc > 0) {
                    result->neworderA_++;
                } else {
                    result->abort_++;
                }
                profiler->mark_neworder_end();
            } else if (transaction_type <= kXctPaymentPercent) {
                profiler->mark_payment_begin();
                if (transaction_payment(connection.get(), payment_prepared_statements.get(), randomGenerator.get(), warehouse_low, warehouse_high, profiler) == 0) result->payment_++; else result->abort_++;
                profiler->mark_payment_end();
            } else if (transaction_type <= kXctOrderStatusPercent) {
                profiler->mark_orderstatus_begin();
                if (transaction_orderstatus(connection.get(), order_status_prepared_statements.get(), randomGenerator.get(), warehouse_low, warehouse_high, profiler) == 0) result->orderstatus_++; else result->abort_++;
                profiler->mark_orderstatus_end();
            } else if (transaction_type <= kXctDelieveryPercent) {
                profiler->mark_delivery_begin();
                if(transaction_delivery(connection.get(), delivery_prepared_statements.get(), randomGenerator.get(), warehouse_low, warehouse_high, profiler) == 0) result->delivery_++; else result->abort_++;
                profiler->mark_delivery_end();
            } else {
                profiler->mark_stocklevel_begin();
                if (transaction_stocklevel(connection.get(), stocklevel_prepared_statements.get(), randomGenerator.get(), warehouse_low, warehouse_high, profiler) == 0) result->stocklevel_++; else result->abort_++;
                profiler->mark_stocklevel_end();
            }
        } catch (std::exception& e) {
            result->abort_++;
            std::cerr << e.what() << std::endl;
        }
        profiler->mark_exit();
    } while(!done);
}

std::uint16_t warehouses;

int driver_main(int argc, char **argv)
{
    gflags::SetUsageMessage("TPC-C implementation for FOEDUS with ogawayama (sharksfin + shakujo front");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    warehouses = FLAGS_warehouse;
#ifdef ENABLE_GOOGLE_PERFTOOLS
    bool performance_measurement = (FLAGS_proffile != "");
#endif

    std::vector<std::thread> threads;
    std::vector<std::unique_ptr<tpcc_result>> results;
    std::vector<std::unique_ptr<tpcc_profiler>> profilers;

    std::time_t t = std::time(nullptr);
    std::tm tm = *std::localtime(&t);

    int pause_milliseconds = 200;
    if (FLAGS_pause_before_exec) pause_milliseconds = 10 * 1000;
    std::this_thread::sleep_for(std::chrono::milliseconds(pause_milliseconds));

    begin = done = false;
    threads.reserve(FLAGS_threads);
    for(int i = 0; i < FLAGS_threads; i++) {
        std::unique_ptr<tpcc_result> result = std::make_unique<tpcc_result>();
        std::unique_ptr<tpcc_profiler> profiler = std::make_unique<tpcc_profiler>();
        threads.emplace_back(tpcc_client, result.get(), i, profiler.get());
        results.emplace_back(std::move(result));
        profilers.emplace_back(std::move(profiler));
    }

    t = std::time(nullptr);
    std::time_t et = t + FLAGS_duration;
    tm = *std::localtime(&t);
    std::cout << std::put_time(&tm, "%c") << " : Executing TPC-C benchmark begin" << std::endl;

    begin = true;
#ifdef ENABLE_GOOGLE_PERFTOOLS
    if (performance_measurement) ProfilerStart((FLAGS_proffile + ".prof").c_str());
#endif
    while(t < et) {
        sleep(et - t);
        t = std::time(nullptr);
    }
#ifdef ENABLE_GOOGLE_PERFTOOLS
    if (performance_measurement) ProfilerStop();
#endif
    done = true;
    t = std::time(nullptr);
    tm = *std::localtime(&t);
    std::cout << std::put_time(&tm, "%c") << " : Executing TPC-C benchmark done" << std::endl;

    for (auto& e : threads) {
        e.join();
    }

    if (FLAGS_ap_profiler) {
        tpcc_profiler prof_total;
        for (auto& e : profilers) {
            prof_total += *e;
        }
        std::cout << "---" << std::endl;
        prof_total.print();
    }
        
    tpcc_result total;
    for (auto& e : results) {
        total += *e;
    }
    if (FLAGS_ap_profiler) std::cout << "---" << std::endl;
    total.print(FLAGS_duration);

    return 0;
}


}  // namespace tpcc
}  // namespace ogawayama

int main([[maybe_unused]] int argc, [[maybe_unused]] char **argv) {
    return ogawayama::tpcc::driver_main(argc, argv);
}
