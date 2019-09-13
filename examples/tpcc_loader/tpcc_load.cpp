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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <cctype>
#include <iostream>
#include <array>

#include "tpcc_common.h"
#include "tpcc_schema.h"
#include "tpcc_transaction.h"
#include "tpcc_scale.h"

namespace ogawayama::tpcc {
    
    /*==================================================================+
      | Load TPCC tables
      +==================================================================*/

    /* Functions */
    int LoadItems(ogawayama::stub::Connection *); // item
    int LoadWare(ogawayama::stub::Connection *); // warehouse, Stock(), District()
    int LoadCust(ogawayama::stub::Connection *); // Customer()
    int LoadOrd(ogawayama::stub::Connection *); // Orders()
    int Stock(ogawayama::stub::Connection *, uint32_t); // stock
    int District(ogawayama::stub::Connection *, uint32_t); // district
    int Customer(ogawayama::stub::Connection *, uint32_t, uint32_t); // customer
    int Orders(ogawayama::stub::Connection *, uint32_t, uint32_t); // orders, new_order, order_line

    /* Global SQL Variables */
    static TIMESTAMP timestamp;
    /* Global Variables */
    
    /* Random generator */
    std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
    
    int
    tpcc_load(ogawayama::stub::Connection * connection)
    {
        gettimestamp(static_cast<char *>(timestamp));
        
        if (LoadItems(connection) != 0) return -1;
        if (LoadWare(connection) != 0) return -1;
        if (LoadCust(connection)!= 0) return -1;
        if (LoadOrd(connection)!= 0) return -1;
        
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | LoadItems
      | DESCRIPTION
      | Loads the Item table
      | ARGUMENTS
      | none
      +==================================================================*/
    int LoadItems(ogawayama::stub::Connection * connection)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t i_id;
        VARCHAR24 i_name;
        double i_price;
        VARCHAR50 i_data;
        int idatasiz;
        std::array<bool, scale::items+1> orig_i{};
        uint32_t pos;
        unsigned int i;
        
        for (i=0; i<scale::items; i++) { orig_i.at(i) = false; }
        for (i=0; i<scale::items/10; i++) {
            do {
                pos = randomGenerator->RandomNumber(1L,scale::items);
            } while (orig_i.at(pos) == true);
            orig_i.at(pos) = true;
        }

        TransactionPtr transaction;
        connection->begin(transaction);
        for (i_id=1; i_id<=scale::items; i_id++) {
            /* Generate Item Data */
            randomGenerator->MakeAlphaString(14,24, static_cast<char *>(i_name));
            i_price=(static_cast<double>(randomGenerator->RandomNumber(100L,10000L)))/100.0;
            idatasiz=randomGenerator->MakeAlphaString(26,50, static_cast<char *>(i_data));
            if (orig_i.at(i_id))
                {
                    pos = randomGenerator->RandomNumber(0L,idatasiz-8);
                    i_data[pos]='o';
                    i_data[pos+1]='r';
                    i_data[pos+2]='i';
                    i_data[pos+3]='g';
                    i_data[pos+4]='i';
                    i_data[pos+5]='n';
                    i_data[pos+6]='a';
                    i_data[pos+7]='l';
                }
            std::string sql = INSERT;
            sql += "ITEM (i_id, i_name, i_price, i_data)";
            sql += VALUESbegin;
            sql += std::to_string(i_id); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(i_name); sql += "'"; sql += COMMA;
            sql += double_to_string(i_price, 2); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(i_data); sql += "'"; sql += VALUESend;
            /*
             * execute_statement()
             */
            transaction->execute_statement(sql);
        }
        transaction->commit();
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | LoadWare
      | DESCRIPTION
      | Loads the Warehouse table
      | Loads Stock, District as Warehouses are created
      | ARGUMENTS
      | none
      +==================================================================*/
    int LoadWare(ogawayama::stub::Connection * connection)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t w_id;
        VARCHAR10 w_name;
        VARCHAR20 w_street_1;
        VARCHAR20 w_street_2;
        VARCHAR20 w_city;
        CHAR2 w_state;
        CHAR9 w_zip;
        double w_tax;
        double w_ytd;
        
        TransactionPtr transaction;
        connection->begin(transaction);
        for (w_id=1L; w_id<=scale::warehouses; w_id++) {
            /* Generate Warehouse Data */
            randomGenerator->MakeAlphaString( 6, 10,  static_cast<char *>(w_name));
            randomGenerator->MakeAddress(static_cast<char *>(w_street_1), static_cast<char *>(w_street_2), static_cast<char *>(w_city), static_cast<char *>(w_state), static_cast<char *>(w_zip));
            w_tax=(static_cast<double>(randomGenerator->RandomNumber(10L,20L)))/100.0;
            w_ytd=3000000.00;
            
            std::string sql = INSERT;
            sql += "WAREHOUSE (w_id, w_name, "
                "w_street_1, w_street_2, w_city, w_state, w_zip, "
                "w_tax, w_ytd)";
            sql += VALUESbegin;
            sql += std::to_string(w_id); sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_name); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_street_1); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_street_2); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_city); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_state); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char*>(w_zip); sql += "'"; sql += COMMA;
            sql += double_to_string(w_tax,2); sql += COMMA;
            sql += double_to_string(w_ytd,2); sql += VALUESend;
            /*
             * execute_statement()
             */
            transaction->execute_statement(sql);
        }
        transaction->commit();
        
        for (w_id=1L; w_id<=scale::warehouses; w_id++) {  
            Stock(connection, w_id);
            District(connection, w_id);
        }
        return 0;
    }
    
    
    /*==================================================================+
      | ROUTINE NAME
      | LoadCust
      | DESCRIPTION
      | Loads the Customer Table
      | ARGUMENTS
      | none
      +==================================================================*/
    int LoadCust(ogawayama::stub::Connection * connection)
    {
        uint32_t w_id;
        uint32_t d_id;
        
        for (w_id=1L; w_id<=scale::warehouses; w_id++) {
            for (d_id=1L; d_id<=scale::districts; d_id++) {
                Customer(connection, d_id,w_id);
            }
        }
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | LoadOrd
      | DESCRIPTION
      | Loads the Orders and Order_Line Tables
      | ARGUMENTS
      | none
      +==================================================================*/
    int LoadOrd(ogawayama::stub::Connection * connection)
    {
        uint32_t w_id;
        uint32_t d_id;

        for (w_id=1L; w_id<=scale::warehouses; w_id++) {
            for (d_id=1L; d_id<=scale::districts; d_id++) {
                Orders(connection, d_id, w_id);
            }
        }
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | Stock
      | DESCRIPTION
      | Loads the Stock table
      | ARGUMENTS
      | w_id - warehouse id
      +==================================================================*/
    int Stock(ogawayama::stub::Connection * connection, uint32_t w_id)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t s_i_id;
        uint32_t s_w_id;
        uint32_t s_quantity;
        VARCHAR50 s_dist_01;
        VARCHAR50 s_dist_02;
        VARCHAR50 s_dist_03;
        VARCHAR50 s_dist_04;
        VARCHAR50 s_dist_05;
        VARCHAR50 s_dist_06;
        VARCHAR50 s_dist_07;
        VARCHAR50 s_dist_08;
        VARCHAR50 s_dist_09;
        VARCHAR50 s_dist_10;
        VARCHAR50 s_data;
        int sdatasiz;
        std::array<uint32_t, scale::items+1> orig_s{};
        uint32_t pos;
        unsigned int i;
        
        s_w_id=w_id;
        for (i=0; i<=scale::items; i++) { orig_s.at(i)=false; }
        for (i=0; i<scale::items/10; i++) {
            do
                {
                    pos=randomGenerator->RandomNumber(1L,scale::items);
                } while (orig_s.at(pos) == true);
            orig_s.at(pos) = true;
        }

        TransactionPtr transaction;
        connection->begin(transaction);
        for (s_i_id=1; s_i_id<=scale::items; s_i_id++) {
            /* Generate Stock Data */
            s_quantity=randomGenerator->RandomNumber(10L,100L);
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_01));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_02));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_03));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_04));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_05));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_06));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_07));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_08));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_09));
            randomGenerator->MakeAlphaString(24,24,static_cast<char *>(s_dist_10));
            sdatasiz=randomGenerator->MakeAlphaString(26,50,static_cast<char *>(s_data));
            if (orig_s.at(s_i_id))
                {
                    pos=randomGenerator->RandomNumber(0L,sdatasiz-8);
                    s_data[pos]='o';
                    s_data[pos+1]='r';
                    s_data[pos+2]='i';
                    s_data[pos+3]='g';
                    s_data[pos+4]='i';
                    s_data[pos+5]='n';
                    s_data[pos+6]='a';
                    s_data[pos+7]='l';
                }
            std::string sql = INSERT;
            sql += "STOCK (s_i_id, s_w_id, s_quantity, "
                "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
                "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, "
                "s_data, s_ytd, s_order_cnt, s_remote_cnt)";
            sql += VALUESbegin;
            sql += std::to_string(s_i_id); sql += COMMA;
            sql += std::to_string(s_w_id); sql += COMMA;
            sql += std::to_string(s_quantity); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_01); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_02); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_03); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_04); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_05); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_06); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_07); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_08); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_09); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_dist_10); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(s_data); sql += "'"; sql += COMMA;
            sql += std::to_string(0); sql += COMMA;
            sql += std::to_string(0); sql += COMMA;
            sql += std::to_string(0); sql += VALUESend;
            /*
             * exexute_statement()
             */
            transaction->execute_statement(sql);
        }
        transaction->commit();
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | District
      | DESCRIPTION
      | Loads the District table
      | ARGUMENTS
      | w_id - warehouse id
      +==================================================================*/
    int District(ogawayama::stub::Connection * connection, uint32_t w_id)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t d_id;
        uint32_t d_w_id;
        VARCHAR10 d_name;
        VARCHAR20 d_street_1;
        VARCHAR20 d_street_2;
        VARCHAR20 d_city;
        CHAR2 d_state;
        CHAR9 d_zip;
        double d_tax;
        double d_ytd;
        uint32_t d_next_o_id;
        d_w_id=w_id;
        d_ytd=30000.0;
        d_next_o_id=scale::orders+1;
        
        TransactionPtr transaction;
        connection->begin(transaction);
        for (d_id=1; d_id<=scale::districts; d_id++) {
            /* Generate District Data */
            randomGenerator->MakeAlphaString(6L,10L,static_cast<char *>(d_name));
            randomGenerator->MakeAddress(static_cast<char *>(d_street_1), static_cast<char *>(d_street_2), static_cast<char *>(d_city), static_cast<char *>(d_state), static_cast<char *>(d_zip));
            d_tax=(static_cast<double>(randomGenerator->RandomNumber(10L,20L)))/100.0;
            std::string sql = INSERT;
            sql += "DISTRICT (d_id, d_w_id, d_name, "
                "d_street_1, d_street_2, d_city, d_state, d_zip, "
                "d_tax, d_ytd, d_next_o_id)";
            sql += VALUESbegin;
            sql += std::to_string(d_id); sql += COMMA;
            sql += std::to_string(d_w_id); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_name); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_street_1); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_street_2); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_city); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_state); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(d_zip); sql += "'"; sql += COMMA;
            sql += double_to_string(d_tax,2); sql += COMMA;
            sql += double_to_string(d_ytd,2); sql += COMMA;
            sql += std::to_string(d_next_o_id); sql += VALUESend;
            /*
             * exexute_statement()
             */
            transaction->execute_statement(sql);
        }
        transaction->commit();
        return 0;
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | Customer
      | DESCRIPTION
      | Loads Customer Table
      | Also inserts corresponding history record
      | ARGUMENTS
      | id - customer id
      | d_id - district id
      | w_id - warehouse id
      +==================================================================*/
    int Customer(ogawayama::stub::Connection *connection, uint32_t d_id, uint32_t w_id)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t c_id;
        uint32_t c_d_id;
        uint32_t c_w_id;
        VARCHAR16 c_first;
        CHAR2 c_middle;
        VARCHAR16 c_last;
        VARCHAR20 c_street_1;
        VARCHAR20 c_street_2;
        VARCHAR20 c_city;
        CHAR2 c_state;
        CHAR9 c_zip;
        CHAR16 c_phone;
        VARCHAR10 c_since;
        CHAR2 c_credit;
        uint32_t c_credit_lim;
        double c_discount;
        double c_balance;
        VARCHAR500 c_data;
        VARCHAR24 h_data;

        TransactionPtr transaction;
        connection->begin(transaction);
        for (c_id=1; c_id<=scale::customers; c_id++) {
            /* Generate Customer Data */
            c_d_id=d_id;
            c_w_id=w_id;
            randomGenerator->MakeAlphaString( 8, 16, static_cast<char *>(c_first) );
            c_middle[0]='O'; c_middle[1]='E'; c_middle[2]='\0';
            if (c_id <= 1000) {
                Lastname(c_id-1,static_cast<char *>(c_last));
            } else {
                Lastname(randomGenerator->nonUniformWithin(255,0,999),static_cast<char *>(c_last));
            }
            randomGenerator->MakeAddress( static_cast<char *>(c_street_1), static_cast<char *>(c_street_2), static_cast<char *>(c_city), static_cast<char *>(c_state), static_cast<char *>(c_zip) );
            randomGenerator->MakeNumberString( 16, 16, static_cast<char *>(c_phone) );
            if (randomGenerator->RandomNumber(0L,1L) > 0) {
                c_credit[0]='G';
            } else {
                c_credit[0]='B';
            }
            c_credit[1]='C'; c_credit[2]='\0';
            c_credit_lim=50000;
            c_discount=(static_cast<double>(randomGenerator->RandomNumber(0L,50L)))/100.0;
            c_balance= -10.0;
            getdatestamp(static_cast<char *>(c_since), randomGenerator->RandomNumber(1L,365L*50L));
            randomGenerator->MakeAlphaString(300,500,static_cast<char *>(c_data));
            std::string sql = INSERT;
            sql += "CUSTOMER (c_id, c_d_id, c_w_id, "
                "c_first, c_middle, c_last, "
                "c_street_1, c_street_2, c_city, c_state, c_zip, "
                "c_phone, c_since, c_credit, "
                "c_credit_lim, c_discount, c_balance, c_data, "
                "c_ytd_payment, c_payment_cnt, c_delivery_cnt) ";
            sql += VALUESbegin;
            sql += std::to_string(c_id); sql += COMMA;
            sql += std::to_string(c_d_id); sql += COMMA;
            sql += std::to_string(c_w_id); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_first); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_middle); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_last); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_street_1); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_street_2); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_city); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_state); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_zip); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_phone); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_since); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_credit); sql += "'"; sql += COMMA;
            sql += double_to_string(c_credit_lim,2); sql += COMMA;
            sql += double_to_string(c_discount,2); sql += COMMA;
            sql += double_to_string(c_balance,2); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_data); sql += "'"; sql += COMMA;
            sql += std::to_string(10.0); sql += COMMA;
            sql += std::to_string(1); sql += COMMA;
            sql += std::to_string(0); sql += VALUESend;
            /*
             * exexute_statement()
             */
            transaction->execute_statement(sql);
            
            sql = INSERT;
            sql += "CUSTOMER_SECONDARY (c_d_id, c_w_id, c_last, c_first, c_id)";
            sql += VALUESbegin;
            sql += std::to_string(c_d_id); sql += COMMA;
            sql += std::to_string(c_w_id); sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_last); sql += "'"; sql += COMMA;
            sql += "'"; sql += static_cast<char *>(c_first); sql += "'"; sql += COMMA;
            sql += std::to_string(c_id); sql += VALUESend;
            /*
             * exexute_statement()
             */
            transaction->execute_statement(sql);
            
            randomGenerator->MakeAlphaString(12,24,static_cast<char *>(h_data));
        }
        transaction->commit();
        return 0;
    }
    
    int GetPermutation(randomGeneratorClass *randomGenerator, std::array<bool, scale::customers> &cid_array)
    {
        while (true) {
            uint32_t r = randomGenerator->RandomNumber(0L, scale::customers-1);
            if (cid_array.at(r)) {       /* This number already taken */
                continue;
            }
            cid_array.at(r) = true;         /* mark taken */
            return r+1;
        }
    }
    
    /*==================================================================+
      | ROUTINE NAME
      | Orders
      | DESCRIPTION
      | Loads the Orders table
      | Also loads the Order_Line table on the fly
      | ARGUMENTS
      | w_id - warehouse id
      +==================================================================*/
    int Orders(ogawayama::stub::Connection *connection, uint32_t d_id, uint32_t w_id)
    {
        //        std::unique_ptr<randomGeneratorClass> randomGenerator = std::make_unique<randomGeneratorClass>();
        
        uint32_t o_id;
        uint32_t o_c_id;
        uint32_t o_d_id;
        uint32_t o_w_id;
        uint32_t o_carrier_id;
        uint32_t o_ol_cnt;
        uint32_t ol;
        uint32_t ol_i_id;
        uint32_t ol_supply_w_id;
        uint32_t ol_quantity;
        double ol_amount;
        VARCHAR24 ol_dist_info;
        o_d_id=d_id;
        o_w_id=w_id;
        std::array<bool, scale::customers> cid_array{};
        for (unsigned int i = 0; i < scale::customers; i++) cid_array.at(i) = false;

        TransactionPtr transaction;
        connection->begin(transaction);
        for (o_id=1; o_id<=scale::orders; o_id++) {
            /* Generate Order Data */
            o_c_id=GetPermutation(randomGenerator.get(), cid_array);
            o_carrier_id=randomGenerator->RandomNumber(1L,10L);
            o_ol_cnt=randomGenerator->RandomNumber(5L,15L);
            if (o_id > ((scale::orders * 7) / 10)) /* the last 900 orders have not been delivered) */
                {
                    std::string sql = INSERT;
                    sql += "ORDERS (o_id, o_c_id, o_d_id, o_w_id, "
                        "o_entry_d, o_ol_cnt, o_all_local)";
                    sql += VALUESbegin;
                    sql += std::to_string(o_id); sql += COMMA;
                    sql += std::to_string(o_c_id); sql += COMMA;
                    sql += std::to_string(o_d_id); sql += COMMA;
                    sql += std::to_string(o_w_id); sql += COMMA;
                    sql += "'"; sql += static_cast<char *>(timestamp); sql += "'"; sql += COMMA;
                    sql += std::to_string(o_ol_cnt); sql += COMMA;
                    sql += std::to_string(1); sql += VALUESend;
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(sql);
                    
                    sql = INSERT;
                    sql += "ORDERS_SECONDARY (o_d_id, o_w_id, o_c_id, o_id)";
                    sql += VALUESbegin;
                    sql += std::to_string(o_d_id); sql += COMMA;
                    sql += std::to_string(o_w_id); sql += COMMA;
                    sql += std::to_string(o_c_id); sql += COMMA;
                    sql += std::to_string(o_id); sql += VALUESend;
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(sql);
                    
                    sql = INSERT;
                    sql += "NEW_ORDER (no_o_id, no_d_id, no_w_id)";
                    sql += VALUESbegin;
                    sql += std::to_string(o_id); sql += COMMA;
                    sql += std::to_string(o_d_id); sql += COMMA;
                    sql += std::to_string(o_w_id); sql += VALUESend;
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(sql);
                }
            else
                {
                    std::string sql = INSERT;
                    sql += "ORDERS (o_id, o_c_id, o_d_id, o_w_id, "
                        "o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)";
                    sql += VALUESbegin;
                    sql += std::to_string(o_id); sql += COMMA;
                    sql += std::to_string(o_c_id); sql += COMMA;
                    sql += std::to_string(o_d_id); sql += COMMA;
                    sql += std::to_string(o_w_id); sql += COMMA;
                    sql += "'"; sql += static_cast<char *>(timestamp); sql += "'"; sql += COMMA;
                    sql += std::to_string(o_carrier_id); sql += COMMA;
                    sql += std::to_string(o_ol_cnt); sql += COMMA;
                    sql += std::to_string(1); sql += VALUESend;
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(sql);
                    
                    sql = INSERT;
                    sql += "ORDERS_SECONDARY (o_d_id, o_w_id, o_c_id, o_id)";
                    sql += VALUESbegin;
                    sql += std::to_string(o_d_id); sql += COMMA;
                    sql += std::to_string(o_w_id); sql += COMMA;
                    sql += std::to_string(o_c_id); sql += COMMA;
                    sql += std::to_string(o_id); sql += VALUESend;
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(sql);
                }
            TIMESTAMP datetime; gettimestamp(static_cast<char *>(datetime), randomGenerator->RandomNumber(1L,90L*24L*60L));
            for (ol=1; ol<=o_ol_cnt; ol++) {
                /* Generate Order Line Data */
                ol_i_id=randomGenerator->RandomNumber(1L,scale::items);
                ol_supply_w_id=o_w_id;
                ol_quantity=5;
                ol_amount=0.0;
                
                randomGenerator->MakeAlphaString(24,24,static_cast<char *>(ol_dist_info));
                
                if (o_id > ((scale::orders * 7) / 10))
                    {
                        std::string sql = INSERT;
                        sql += "ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, "
                            "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
                            "ol_dist_info)";
                        sql += VALUESbegin;
                        sql += std::to_string(o_id); sql += COMMA;
                        sql += std::to_string(o_d_id); sql += COMMA;
                        sql += std::to_string(o_w_id); sql += COMMA;
                        sql += std::to_string(ol); sql += COMMA;
                        sql += std::to_string(ol_i_id); sql += COMMA;
                        sql += std::to_string(ol_supply_w_id); sql += COMMA;
                        sql += std::to_string(ol_quantity); sql += COMMA;
                        sql += double_to_string(ol_amount,2); sql += COMMA;
                        sql += "'"; sql += static_cast<char *>(ol_dist_info); sql += "'"; sql += VALUESend;
                        /*
                         * exexute_statement()
                         */
                        transaction->execute_statement(sql);
                    }
                else
                    {
                        ol_amount = (static_cast<double>(randomGenerator->RandomNumber(10L, 10000L)))/100.0;
                        std::string sql = INSERT;
                        sql += "ORDER_LINE (ol_o_id, ol_d_id, ol_w_id, ol_number, "
                            "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
                            "ol_dist_info, ol_delivery_d)";
                        sql += VALUESbegin;
                        sql += std::to_string(o_id); sql += COMMA;
                        sql += std::to_string(o_d_id); sql += COMMA;
                        sql += std::to_string(o_w_id); sql += COMMA;
                        sql += std::to_string(ol); sql += COMMA;
                        sql += std::to_string(ol_i_id);     sql += COMMA;
                        sql += std::to_string(ol_supply_w_id); sql += COMMA;
                        sql += std::to_string(ol_quantity); sql += COMMA;
                        sql += double_to_string(ol_amount,2); sql += COMMA;
                        sql += "'"; sql += static_cast<char *>(ol_dist_info); sql += "'"; sql += COMMA;
                        sql += "'"; sql += static_cast<char *>(datetime); sql += "'"; sql += VALUESend;
                        /*
                         * exexute_statement()
                         */
                        transaction->execute_statement(sql);
                    }
            }
        }
        transaction->commit();
        return 0;
    }
    
}  // namespace ogawayama::tpcc
