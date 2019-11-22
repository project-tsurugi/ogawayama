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

#include "tpcc_common.h"
#include "tpcc_schema.h"

namespace ogawayama {
namespace tpcc {

std::vector<std::string_view> sql_create_table = {  // NOLINT
    "CREATE TABLE warehouse ("
    "w_id INT NOT NULL, "
    "w_name VARCHAR(10) NOT NULL, "
    "w_street_1 VARCHAR(20) NOT NULL, "
    "w_street_2 VARCHAR(20) NOT NULL, "
    "w_city VARCHAR(20) NOT NULL, "
    "w_state CHAR(2) NOT NULL, "
    "w_zip CHAR(9) NOT NULL, "
    "w_tax DOUBLE NOT NULL, "
    "w_ytd DOUBLE NOT NULL, "
    "PRIMARY KEY(w_id))",

    "CREATE TABLE district ("
    "d_id INT NOT NULL, "
    "d_w_id INT NOT NULL, "
    "d_name VARCHAR(10) NOT NULL, "
    "d_street_1 VARCHAR(20) NOT NULL, "
    "d_street_2 VARCHAR(20) NOT NULL, "
    "d_city VARCHAR(20) NOT NULL, "
    "d_state CHAR(2) NOT NULL, "
    "d_zip  CHAR(9) NOT NULL, "
    "d_tax DOUBLE NOT NULL, "
    "d_ytd DOUBLE NOT NULL, "
    "d_next_o_id INT NOT NULL, "
    "PRIMARY KEY(d_w_id, d_id))",

    "CREATE TABLE customer ("
    "c_id INT NOT NULL, "
    "c_d_id INT NOT NULL, "
    "c_w_id INT NOT NULL, "
    "c_first VARCHAR(16) NOT NULL, "
    "c_middle CHAR(2) NOT NULL, "
    "c_last VARCHAR(16) NOT NULL, "
    "c_street_1 VARCHAR(20) NOT NULL, "
    "c_street_2 VARCHAR(20) NOT NULL, "
    "c_city VARCHAR(20) NOT NULL, "
    "c_state CHAR(2) NOT NULL, "
    "c_zip  CHAR(9) NOT NULL, "
    "c_phone CHAR(16) NOT NULL, "
    "c_since CHAR(24) NOT NULL, " // date
    "c_credit CHAR(2) NOT NULL, "
    "c_credit_lim DOUBLE NOT NULL, "
    "c_discount DOUBLE NOT NULL, "
    "c_balance DOUBLE NOT NULL, "
    "c_ytd_payment DOUBLE NOT NULL, "
    "c_payment_cnt INT NOT NULL, "
    "c_delivery_cnt INT NOT NULL, "
    "c_data VARCHAR(500) NOT NULL, "
    "PRIMARY KEY(c_w_id, c_d_id, c_id))",

    "CREATE TABLE customer_secondary ("
    "c_d_id INT NOT NULL, "
    "c_w_id INT NOT NULL, "
    "c_last VARCHAR(16) NOT NULL, "
    "c_first VARCHAR(16) NOT NULL, "
    "c_id INT NOT NULL, "
    "PRIMARY KEY(c_w_id, c_d_id, c_last, c_first))",
    
    "CREATE TABLE new_order ("
    "no_o_id INT NOT NULL, "
    "no_d_id INT NOT NULL, "
    "no_w_id INT NOT NULL, "
    "PRIMARY KEY(no_w_id, no_d_id, no_o_id))",
    
    "CREATE TABLE orders (" // ORDER is a reserved word of SQL
    "o_id INT NOT NULL, "
    "o_d_id INT NOT NULL, "
    "o_w_id INT NOT NULL, "
    "o_c_id INT NOT NULL, "
    "o_entry_d CHAR(24) NOT NULL, " // date
    "o_carrier_id INT, " // nullable
    "o_ol_cnt INT NOT NULL, "
    "o_all_local INT NOT NULL, "
    "PRIMARY KEY(o_w_id, o_d_id, o_id))",
    
    "CREATE TABLE orders_secondary ("
    "o_d_id INT NOT NULL, "
    "o_w_id INT NOT NULL, "
    "o_c_id INT NOT NULL, "
    "o_id INT NOT NULL, "
    "PRIMARY KEY(o_w_id, o_d_id, o_c_id, o_id))",
    
    "CREATE TABLE order_line ("
    "ol_o_id INT NOT NULL, "
    "ol_d_id INT NOT NULL, "
    "ol_w_id INT NOT NULL, "
    "ol_number INT NOT NULL, "
    "ol_i_id INT NOT NULL, "
    "ol_supply_w_id INT NOT NULL, "
    "ol_delivery_d CHAR(24), " // date, nullable
    "ol_quantity INT NOT NULL, "
    "ol_amount DOUBLE NOT NULL, "
    "ol_dist_info CHAR(24) NOT NULL, "
    "PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number))",
    
    "CREATE TABLE item ("
    "i_id INT NOT NULL, "
    "i_im_id INT, " // not used
    "i_name VARCHAR(24) NOT NULL, "
    "i_price DOUBLE NOT NULL, "
    "i_data VARCHAR(50) NOT NULL, "
    "PRIMARY KEY(i_id))",
    
    "CREATE TABLE stock ("
    "s_i_id INT NOT NULL, "
    "s_w_id INT NOT NULL, "
    "s_quantity INT NOT NULL, "
    "s_dist_01 CHAR(24) NOT NULL, "
    "s_dist_02 CHAR(24) NOT NULL, "
    "s_dist_03 CHAR(24) NOT NULL, "
    "s_dist_04 CHAR(24) NOT NULL, "
    "s_dist_05 CHAR(24) NOT NULL, "
    "s_dist_06 CHAR(24) NOT NULL, "
    "s_dist_07 CHAR(24) NOT NULL, "
    "s_dist_08 CHAR(24) NOT NULL, "
    "s_dist_09 CHAR(24) NOT NULL, "
    "s_dist_10 CHAR(24) NOT NULL, "
    "s_ytd INT NOT NULL, "
    "s_order_cnt INT NOT NULL, "
    "s_remote_cnt INT NOT NULL, "
    "s_data VARCHAR(50) NOT NULL, "
    "PRIMARY KEY(s_w_id, s_i_id))"
};

    int
    tpcc_tables(ogawayama::stub::Connection * connection)
    {
        TransactionPtr transaction;
        connection->begin(transaction);

        for (auto& sql : sql_create_table) {
            transaction->execute_statement(sql);
        }

        transaction->commit();
        return 0;
    }



}  // namespace tpcc
}  // namespace ogawayama
