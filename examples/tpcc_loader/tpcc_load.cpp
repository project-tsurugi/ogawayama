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
        std::vector<bool> orig_i;
        uint32_t pos;
        unsigned int i;
        
        orig_i.resize(scale->items+1);
        for (i=0; i<scale->items; i++) { orig_i.at(i) = false; }
        for (i=0; i<scale->items/10; i++) {
            do {
                pos = randomGenerator->RandomNumber(1L,scale->items);
            } while (orig_i.at(pos) == true);
            orig_i.at(pos) = true;
        }

        static PreparedStatementPtr prepared_item;
        if (connection->prepare("INSERT INTO item (i_id, i_name, i_price, i_data) VALUES (:p1, :p2, :p3, :p4)", prepared_item) != ERROR_CODE::OK) {
                                                                                                  // :i_id, :i_name, :i_price, :i_data
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (i_id=1; i_id<=scale->items; i_id++) {
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

            prepared_item->set_parameter(static_cast<std::int32_t>(i_id));
            prepared_item->set_parameter(i_name);
            prepared_item->set_parameter(i_price);
            prepared_item->set_parameter(i_data);
            /*
             * execute_statement()
             */
            transaction->execute_statement(prepared_item.get());
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
        
        static PreparedStatementPtr prepared_warehouse;
        if (connection->prepare("INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) "
                                "VALUES (:p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9)", prepared_warehouse) != ERROR_CODE::OK) {
                                // :w_id, :w_name, :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_tax, :w_ytd
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (w_id=1L; w_id<=scale->warehouses; w_id++) {
            /* Generate Warehouse Data */
            randomGenerator->MakeAlphaString( 6, 10,  static_cast<char *>(w_name));
            randomGenerator->MakeAddress(static_cast<char *>(w_street_1), static_cast<char *>(w_street_2), static_cast<char *>(w_city), static_cast<char *>(w_state), static_cast<char *>(w_zip));
            w_tax=(static_cast<double>(randomGenerator->RandomNumber(10L,20L)))/100.0;
            w_ytd=3000000.00;
            
            prepared_warehouse->set_parameter(static_cast<std::int32_t>(w_id));
            prepared_warehouse->set_parameter(w_name);
            prepared_warehouse->set_parameter(w_street_1);
            prepared_warehouse->set_parameter(w_street_2);
            prepared_warehouse->set_parameter(w_city);
            prepared_warehouse->set_parameter(w_state);
            prepared_warehouse->set_parameter(w_zip);
            prepared_warehouse->set_parameter(w_tax);
            prepared_warehouse->set_parameter(w_ytd);
            /*
             * execute_statement()
             */
            transaction->execute_statement(prepared_warehouse.get());
        }
        transaction->commit();
        
        for (w_id=1L; w_id<=scale->warehouses; w_id++) {  
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
        
        for (w_id=1L; w_id<=scale->warehouses; w_id++) {
            for (d_id=1L; d_id<=scale->districts; d_id++) {
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

        for (w_id=1L; w_id<=scale->warehouses; w_id++) {
            for (d_id=1L; d_id<=scale->districts; d_id++) {
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
        std::vector<bool> orig_s;
        uint32_t pos;
        unsigned int i;
        
        orig_s.resize(scale->items+1);
        s_w_id=w_id;
        for (i=0; i<=scale->items; i++) { orig_s.at(i)=false; }
        for (i=0; i<scale->items/10; i++) {
            do
                {
                    pos=randomGenerator->RandomNumber(1L,scale->items);
                } while (orig_s.at(pos) == true);
            orig_s.at(pos) = true;
        }

        static PreparedStatementPtr prepared_stock;
        if (connection->prepare(
            "INSERT INTO "
            "stock (s_i_id, s_w_id, s_quantity, "
            "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, "
            "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, "
            "s_data, s_ytd, s_order_cnt, s_remote_cnt)"
            "VALUES (:p1, :p2, :p3, "
            ":p4, :p5, :p6, :p7, :p8, "
            ":p9, :p10, :p11, :p12, :p13, "
            ":p14, 0, 0, 0)",
            prepared_stock) != ERROR_CODE::OK) {
            //            "VALUES (:s_i_id, :s_w_id, :s_quantity, "
            //            ":s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05, "
            //            ":s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10, "
            //            ":s_data, 0, 0, 0)"
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (s_i_id=1; s_i_id<=scale->items; s_i_id++) {
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
            prepared_stock->set_parameter(static_cast<std::int32_t>(s_i_id));
            prepared_stock->set_parameter(static_cast<std::int32_t>(s_w_id));
            prepared_stock->set_parameter(static_cast<std::int32_t>(s_quantity));
            prepared_stock->set_parameter(s_dist_01);
            prepared_stock->set_parameter(s_dist_02);
            prepared_stock->set_parameter(s_dist_03);
            prepared_stock->set_parameter(s_dist_04);
            prepared_stock->set_parameter(s_dist_05);
            prepared_stock->set_parameter(s_dist_06);
            prepared_stock->set_parameter(s_dist_07);
            prepared_stock->set_parameter(s_dist_08);
            prepared_stock->set_parameter(s_dist_09);
            prepared_stock->set_parameter(s_dist_10);
            prepared_stock->set_parameter(s_data);
            /*
             * exexute_statement()
             */
            transaction->execute_statement(prepared_stock.get());
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
        d_next_o_id=scale->orders+1;

        static PreparedStatementPtr prepared_district;
        if (connection->prepare(
                "INSERT INTO "
                "district (d_id, d_w_id, d_name, "
                "d_street_1, d_street_2, d_city, d_state, d_zip, "
                "d_tax, d_ytd, d_next_o_id)"
                "VALUES (:p1, :p2, :p3, "
                ":p4, :p5, :p6, :p7, :p8, "
                ":p9, :p10, :p11)",
                prepared_district) != ERROR_CODE::OK) {
            //                "VALUES (:d_id, :d_w_id, :d_name, "
            //                ":d_street_1, :d_street_2, :d_city, :d_state, :d_zip, "
            //                ":d_tax, :d_ytd, :d_next_o_id)",
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (d_id=1; d_id<=scale->districts; d_id++) {
            /* Generate District Data */
            randomGenerator->MakeAlphaString(6L,10L,static_cast<char *>(d_name));
            randomGenerator->MakeAddress(static_cast<char *>(d_street_1), static_cast<char *>(d_street_2), static_cast<char *>(d_city), static_cast<char *>(d_state), static_cast<char *>(d_zip));
            d_tax=(static_cast<double>(randomGenerator->RandomNumber(10L,20L)))/100.0;
            
            prepared_district->set_parameter(static_cast<std::int32_t>(d_id));
            prepared_district->set_parameter(static_cast<std::int32_t>(d_w_id));
            prepared_district->set_parameter(d_name);
            prepared_district->set_parameter(d_street_1);
            prepared_district->set_parameter(d_street_2);
            prepared_district->set_parameter(d_city);
            prepared_district->set_parameter(d_state);
            prepared_district->set_parameter(d_zip);
            prepared_district->set_parameter(d_tax);
            prepared_district->set_parameter(d_ytd);
            prepared_district->set_parameter(static_cast<std::int32_t>(d_next_o_id));
            /*
             * exexute_statement()
             */
            transaction->execute_statement(prepared_district.get());
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
        //        VARCHAR24 h_data;

        static PreparedStatementPtr prepared_customer;
        if (connection->prepare(
            "INSERT INTO customer "
            "(c_id, c_d_id, c_w_id, "
            "c_first, c_middle, c_last, "
            "c_street_1, c_street_2, c_city, c_state, c_zip, "
            "c_phone, c_since, c_credit, "
            "c_credit_lim, c_discount, c_balance, c_data, "
            "c_ytd_payment, c_payment_cnt, c_delivery_cnt) "
            "VALUES (:p1, :p2, :p3, "
            ":p4, :p5, :p6, "
            ":p7, :p8, :p9, :p10, :p11, "
            ":p12, :p13, :p14, "
            ":p15, :p16, :p17, :p18, "
            "10.0, 1, 0)",
            prepared_customer) != ERROR_CODE::OK) {
            //            "VALUES (:c_id, :c_d_id, :c_w_id, "
            //            ":c_first, :c_middle, :c_last, "
            //            ":c_street_1, :c_street_2, :c_city, :c_state, :c_zip, "
            //            ":c_phone, :c_since, :c_credit, "
            //            ":c_credit_lim, :c_discount, :c_balance, :c_data, "
            //            "10.0, 1, 0) ");
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        static PreparedStatementPtr prepared_customer_secondary;
        if (connection->prepare(
            "INSERT INTO "
            "customer_secondary (c_d_id, c_w_id, c_last, c_first, c_id)"
            "VALUES (:p1, :p2, :p3, :p4, :p5)", prepared_customer_secondary) != ERROR_CODE::OK) {
            //            "VALUES (:c_d_id, :c_w_id, :c_last, :c_first, :c_id)");
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (c_id=1; c_id<=scale->customers; c_id++) {
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

            prepared_customer->set_parameter(static_cast<std::int32_t>(c_id));
            prepared_customer->set_parameter(static_cast<std::int32_t>(c_d_id));
            prepared_customer->set_parameter(static_cast<std::int32_t>(c_w_id));
            prepared_customer->set_parameter(c_first);
            prepared_customer->set_parameter(c_middle);
            prepared_customer->set_parameter(c_last);
            prepared_customer->set_parameter(c_street_1);
            prepared_customer->set_parameter(c_street_2);
            prepared_customer->set_parameter(c_city);
            prepared_customer->set_parameter(c_state);
            prepared_customer->set_parameter(c_zip);
            prepared_customer->set_parameter(c_phone);
            prepared_customer->set_parameter(c_since);
            prepared_customer->set_parameter(c_credit);
            prepared_customer->set_parameter(static_cast<double>(c_credit_lim));
            prepared_customer->set_parameter(c_discount);
            prepared_customer->set_parameter(c_balance);
            prepared_customer->set_parameter(c_data);
            /*
             * exexute_statement()
             */
            transaction->execute_statement(prepared_customer.get());

            prepared_customer_secondary->set_parameter(static_cast<std::int32_t>(c_d_id));
            prepared_customer_secondary->set_parameter(static_cast<std::int32_t>(c_w_id));
            prepared_customer_secondary->set_parameter(c_last);
            prepared_customer_secondary->set_parameter(c_first);
            prepared_customer_secondary->set_parameter(static_cast<std::int32_t>(c_id));
            /*
             * exexute_statement()
             */
            transaction->execute_statement(prepared_customer_secondary.get());
            
            //            randomGenerator->MakeAlphaString(12,24,static_cast<char *>(h_data));
        }
        transaction->commit();
        return 0;
    }
    
    int GetPermutation(randomGeneratorClass *randomGenerator, std::vector<bool> &cid_array)
    {
        while (true) {
            uint32_t r = randomGenerator->RandomNumber(0L, scale->customers-1);
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
        std::vector<bool> cid_array;

        cid_array.resize(scale->customers);
        for (unsigned int i = 0; i < scale->customers; i++) cid_array.at(i) = false;

        static PreparedStatementPtr prepared_orders;
        if (connection->prepare(
            "INSERT INTO "
            "orders (o_id, o_c_id, o_d_id, o_w_id, "
            "o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)"
            "VALUES (:p1, :p2, :p3, :p4, "
            ":p5, :p6, :p7, :p8)",
            prepared_orders) != ERROR_CODE::OK) {
            //            "VALUES (:o_id, :o_c_id, :o_d_id, :o_w_id, "
            //            ":o_entry_d, :o_carrier_id, :o_ol_cnt, :o_all_local)");                                
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        static PreparedStatementPtr prepared_orders_secondary;
        if (connection->prepare(
            "INSERT INTO "
            "orders_secondary (o_d_id, o_w_id, o_c_id, o_id) "
            "VALUES (:p1, :p2, :p3, :p4)",
            prepared_orders_secondary) != ERROR_CODE::OK) {
            //            "VALUES (:o_d_id, :o_w_id, :o_c_id, :o_id)");
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        static PreparedStatementPtr prepared_neworder;
        if (connection->prepare(
            "INSERT INTO "
            "new_order (no_o_id, no_d_id, no_w_id)"
            "VALUES (:p1, :p2, :p3)",
            prepared_neworder) != ERROR_CODE::OK) {
            //            "VALUES (:no_o_id, :no_d_id, :no_w_id)");
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        static PreparedStatementPtr prepared_orderline;
        if (connection->prepare(
            "INSERT INTO "
            "order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, "
            "ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, "
            "ol_dist_info, ol_delivery_d)"
            "VALUES (:p1, :p2, :p3, :p4, "
            ":p5, :p6, :p7, :p8, "
            ":p9, :p10)",
            prepared_orderline) != ERROR_CODE::OK) {
            //            "VALUES (:ol_o_id, :ol_d_id, :ol_w_id, :ol_number, "
            //            ":ol_i_id, :ol_supply_w_id, :ol_quantity, :ol_amount, "
            //            ":ol_dist_info, :ol_delivery_d)",
            std::cerr << __func__ << ":" << __LINE__ << std::endl; _exit(-1);
        }
        TransactionPtr transaction;
        connection->begin(transaction);
        for (o_id=1; o_id<=scale->orders; o_id++) {
            /* Generate Order Data */
            o_c_id=GetPermutation(randomGenerator.get(), cid_array);
            o_carrier_id=randomGenerator->RandomNumber(1L,10L);
            o_ol_cnt=randomGenerator->RandomNumber(5L,15L);
            if (o_id > ((scale->orders * 7) / 10)) /* the last 900 orders have not been delivered) */
                {
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_c_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_d_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_w_id));
                    prepared_orders->set_parameter(timestamp);
                    prepared_orders->set_parameter();
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_ol_cnt));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(1));
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(prepared_orders.get());

                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_d_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_w_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_c_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_id));
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(prepared_orders_secondary.get());

                    prepared_neworder->set_parameter(static_cast<std::int32_t>(o_id));
                    prepared_neworder->set_parameter(static_cast<std::int32_t>(o_d_id));
                    prepared_neworder->set_parameter(static_cast<std::int32_t>(o_w_id));
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(prepared_neworder.get());
                }
            else
                {
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_c_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_d_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_w_id));
                    prepared_orders->set_parameter(timestamp);
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_carrier_id));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(o_ol_cnt));
                    prepared_orders->set_parameter(static_cast<std::int32_t>(1));
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(prepared_orders.get());
                    
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_d_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_w_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_c_id));
                    prepared_orders_secondary->set_parameter(static_cast<std::int32_t>(o_id));
                    /*
                     * exexute_statement()
                     */
                    transaction->execute_statement(prepared_orders_secondary.get());
                }
            TIMESTAMP datetime; gettimestamp(static_cast<char *>(datetime), randomGenerator->RandomNumber(1L,90L*24L*60L));
            for (ol=1; ol<=o_ol_cnt; ol++) {
                /* Generate Order Line Data */
                ol_i_id=randomGenerator->RandomNumber(1L,scale->items);
                ol_supply_w_id=o_w_id;
                ol_quantity=5;
                ol_amount=0.0;
                
                randomGenerator->MakeAlphaString(24,24,static_cast<char *>(ol_dist_info));
                
                if (o_id > ((scale->orders * 7) / 10))
                    {
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_d_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_w_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_i_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_supply_w_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_quantity));
                        prepared_orderline->set_parameter(ol_amount);
                        prepared_orderline->set_parameter(ol_dist_info);
                        prepared_orderline->set_parameter();
                        /*
                         * exexute_statement()
                         */
                        transaction->execute_statement(prepared_orderline.get());
                    }
                else
                    {
                        ol_amount = (static_cast<double>(randomGenerator->RandomNumber(10L, 10000L)))/100.0;

                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_d_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(o_w_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_i_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_supply_w_id));
                        prepared_orderline->set_parameter(static_cast<std::int32_t>(ol_quantity));
                        prepared_orderline->set_parameter(ol_amount);
                        prepared_orderline->set_parameter(ol_dist_info);
                        prepared_orderline->set_parameter(datetime);
                        /*
                         * exexute_statement()
                         */
                        transaction->execute_statement(prepared_orderline.get());
                    }
            }
        }
        transaction->commit();
        return 0;
    }
    
}  // namespace ogawayama::tpcc
