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

#ifndef TPCC_SCHEMA_H_
#define TPCC_SCHEMA_H_

#include <cstdint>

namespace ogawayama {
namespace tpcc {

  // Add one byete to store the delimiter ('\0')
  using VARCHAR10 = char[11];
  using VARCHAR16 = char[17];
  using VARCHAR20 = char[21];
  using VARCHAR24 = char[25];
  using VARCHAR50 = char[51];
  using VARCHAR500 = char[501];

  using CHAR2 = char[3];
  using CHAR9 = char[10];
  using CHAR16 = char[17];
  using CHAR24 = char[25];
  using TIMESTAMP = char[26];
  
  using DOUBLE = double;
  using SMALLINT = uint32_t;  // for the time being
  using INTEGER = uint32_t;

}  // Namespace tpcc
}  // namespace ogawayama

#endif // C_SCHEMA_H_
