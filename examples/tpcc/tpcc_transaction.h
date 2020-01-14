/*
 * Copyright 2018-2020 tsurugi project.
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

#include <array>

#include "tpcc_schema.h"
#include "tpcc_scale.h"

namespace ogawayama {
namespace tpcc {

  struct neworderParams {
    SMALLINT w_id;
    SMALLINT d_id;
    INTEGER c_id;
    INTEGER ol_cnt;
    bool will_rollback;
    std::array<INTEGER, scale::max_ol> qty;
    std::array<INTEGER, scale::max_ol> itemid;
    bool remoteWarehouse;
    INTEGER all_local;
    SMALLINT supply_wid;
    //    INTEGER supware[kOlMax];  // unclear: should we use supply_wid or supware[kOlMax]?
    TIMESTAMP entry_d;
    bool all_local_warehouse;
  };
  struct paymentParams {
    SMALLINT w_id;
    SMALLINT d_id;
    DOUBLE h_amount;
    bool byname;
    INTEGER c_id;
    VARCHAR16 c_last;
    char h_date[11]; // unclear: is it OK to set date like '2018-01-21' to this data?
    char h_data[25]; // unclear: how to prepare this data?
  };
  struct orderstatusParams {
    SMALLINT w_id;
    SMALLINT d_id;
    bool byname;
    INTEGER c_id;
    VARCHAR16 c_last;
  };
  struct deliveryParams {
    SMALLINT w_id;
    SMALLINT o_carrier_id;
    TIMESTAMP ol_delivery_d;
  };
  struct stocklevelParams {
    SMALLINT w_id;
    SMALLINT d_id;
    INTEGER threshold;
  };

  int transaction_neworder(Database *, prepared_statements *, randomGeneratorClass *, std::uint16_t, std::uint16_t, tpcc_profiler *);
  int transaction_payment(Database *, prepared_statements *, randomGeneratorClass *, std::uint16_t, std::uint16_t, tpcc_profiler *);
  int transaction_orderstatus(Database *, prepared_statements *, randomGeneratorClass *, std::uint16_t, std::uint16_t, tpcc_profiler *);
  int transaction_delivery(Database *, prepared_statements *, randomGeneratorClass *, std::uint16_t, std::uint16_t, tpcc_profiler *);
  int transaction_stocklevel(Database *, prepared_statements *, randomGeneratorClass *, std::uint16_t, std::uint16_t, tpcc_profiler *);

}  // namespace tpcc
}  // namespace ogawayama
