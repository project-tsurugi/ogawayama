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


#ifndef TPCC_TRANSACTION_H_
#define TPCC_TRANSACTION_H_

#include <array>

#include "ogawayama/stub/api.h"
#include "tpcc_schema.h"
#include "tpcc_scale.h"

namespace ogawayama {
namespace tpcc {

  int tpcc_tables(ogawayama::stub::Connection * connection);
  int tpcc_load(ogawayama::stub::Connection * connection);

#define SELECT "SELECT * FROM ";
#define WHERE " WHERE ";
#define UPDATE "UPDATE ";
#define DELETE "DELETE FROM ";
#define AND " AND ";
#define SET " SET ";
#define INSERT "INSERT INTO ";
#define VALUESbegin " VALUES (";
#define VALUESend ")";
#define COMMA ", ";
  
}  // namespace tpcc
}  // namespace ogawayama

#endif  // TPCC_TRANSACTION_H_
