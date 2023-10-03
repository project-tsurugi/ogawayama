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

#include <array>

#include "glog/logging.h"

#include "ogawayama/stub/api.h"

#include "tpcc_common.h"
#include "tpcc_transaction.h"


namespace ogawayama {
namespace tpcc {

/*
 * New Order transaction SQL statements.
 */

#define NEW_ORDER_1 statements->get_preprad_statement(0)
#define NEW_ORDER_2 statements->get_preprad_statement(1)
#define NEW_ORDER_3 statements->get_preprad_statement(2)
#define NEW_ORDER_4 statements->get_preprad_statement(3)
#define NEW_ORDER_5 statements->get_preprad_statement(4)
#define NEW_ORDER_6 statements->get_preprad_statement(5)
#define NEW_ORDER_7 statements->get_preprad_statement(6)
// #define NEW_ORDER_8 statements->get_preprad_statement(7)
#define NEW_ORDER_8x(index) statements->get_preprad_statement((7-1)+index)
#define NEW_ORDER_9 statements->get_preprad_statement(17)
#define NEW_ORDER_10 statements->get_preprad_statement(18)
#define NEW_ORDER_61 statements->get_preprad_statement(19)

const char s_dist[10][11] = {
	"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
	"s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10"
};

cached_statement new_order_statements[] =
{
	{ /* NEW_ORDER_1 (0) */
	"SELECT w_tax\n" \
	"FROM warehouse\n" \
	"WHERE w_id = :p1",
	},

	{ /* NEW_ORDER_2 (1) */
	"SELECT d_tax, d_next_o_id\n" \
	"FROM district \n" \
	"WHERE d_w_id = :p1\n" \
	" AND d_id = :p2\n",
	},

	{ /* NEW_ORDER_3 (2) */
	"UPDATE district\n" \
	"SET d_next_o_id = d_next_o_id + 1\n" \
	"WHERE d_w_id = :p1\n" \
	"  AND d_id = :p2",
	},

	{ /* NEW_ORDER_4 (3) */
	"SELECT c_discount, c_last, c_credit\n" \
	"FROM customer\n" \
	"WHERE c_w_id = :p1\n" \
	"  AND c_d_id = :p2\n" \
	"  AND c_id = :p3",
	},

	{ /* NEW_ORDER_5 (4) */
	"INSERT INTO new_order (no_o_id, no_w_id, no_d_id)\n" \
	"VALUES (:p1, :p2, :p3)",
	},

	{ /* NEW_ORDER_6 (5) */
	"INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d,\n" \
	"		    o_ol_cnt, o_all_local)\n" \
	"VALUES (:p1, :p2, :p3, :p4, :p5, :p6, :p7)",
        // :p7 = current_timestamp
	},

	{ /* NEW_ORDER_7 (6) */
	"SELECT i_price, i_name, i_data\n" \
	"FROM item\n" \
	"WHERE i_id = :p1",
	},
#if 0
	{ /* NEW_ORDER_8 */
	"SELECT s_quantity, :p1, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p2\n" \
	"  AND s_w_id = :p3",
	},
#endif
	{ /* NEW_ORDER_8_01 (7) */
	"SELECT s_quantity, s_dist_01, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_02 (8) */
	"SELECT s_quantity, s_dist_02, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_03 (9) */
	"SELECT s_quantity, s_dist_03, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_04 (10) */
	"SELECT s_quantity, s_dist_04, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_05 (11) */
	"SELECT s_quantity, s_dist_05, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_06 (12) */
	"SELECT s_quantity, s_dist_06, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_07 (13) */
	"SELECT s_quantity, s_dist_07, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_08 (14) */
	"SELECT s_quantity, s_dist_08, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_09 (15) */
	"SELECT s_quantity, s_dist_09, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_8_10 (16) */
	"SELECT s_quantity, s_dist_10, s_data\n" \
	"FROM stock\n" \
	"WHERE s_i_id = :p1\n" \
	"  AND s_w_id = :p2",
	},

	{ /* NEW_ORDER_9 (17) */
	"UPDATE stock\n" \
	"SET s_quantity = s_quantity - :p1\n" \
	"WHERE s_i_id = :p2\n" \
	"  AND s_w_id = :p3",
	},

	{ /* NEW_ORDER_10 (18) */
	"INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,\n" \
	"			ol_i_id, ol_supply_w_id, ol_delivery_d,\n" \
	"			ol_quantity, ol_amount, ol_dist_info)\n" \
	"VALUES (:p1, :p2, :p3, :p4, :p5, :p6, NULL, :p7, :p8, :p9)",
	},

	{ /* NEW_ORDER_61 (19) */
            "INSERT INTO orders_secondary (o_d_id, o_w_id, o_c_id, o_id)\n"
            "VALUES (:p1, :p2, :p3, :p4)\n",
	},

	{ NULL }
};


int
transaction_neworder(ConnectionPtr::element_type *connection, prepared_statements *statements, randomGeneratorClass *randomGenerator, std::uint16_t warehouse_low, std::uint16_t warehouse_high, [[maybe_unused]] tpcc_profiler *profiler)
{
    neworderParams params = {};
    //    std::array<INTEGER, scale::max_ol> stock = {};
    //    std::array<char, scale::max_ol> bg = {};
    //    std::array<DOUBLE, scale::max_ol> amt = {};
    //    DOUBLE total;
    //    std::array<DOUBLE, scale::max_ol> price = {};
    //    std::array<VARCHAR24, scale::max_ol> iname = {};
    //    int nLoops = 0;
    
    //  bool all_local_warehouse;
    
    params.w_id = randomGenerator->uniformWithin(warehouse_low, warehouse_high);
    params.d_id = randomGenerator->uniformWithin(1, scale::districts);
    params.c_id = randomGenerator->uniformWithin(1, scale::customers);
    params.ol_cnt = randomGenerator->uniformWithin(scale::min_ol_count,
        scale::max_ol_count);
    
    params.remoteWarehouse = (randomGenerator->uniformWithin(1, 100) <= kNewOrderRemotePercent);
    if (params.remoteWarehouse && warehouses > 1U) {
        do {
            params.supply_wid = randomGenerator->uniformWithin (1, warehouses);
        } while (params.supply_wid != params.w_id);
        params.all_local_warehouse = false;
    } else {
        params.supply_wid = params.w_id;
        params.all_local_warehouse = true;
    }
    for (unsigned int ol = 1; ol <= params.ol_cnt; ++ol) {
        params.qty.at(ol - 1) = randomGenerator->uniformWithin(1, 10);
        params.itemid.at(ol - 1) = randomGenerator->nonUniformWithin(8191, 1, scale::items);
    }
    gettimestamp(static_cast<char *>(params.entry_d));
    params.will_rollback = (randomGenerator->uniformWithin(1, 100) == 1);

        TransactionPtr transaction;
        ResultSetPtr result_set{};
        //        MetadataPtr metadata;
        ERROR_CODE err;

	/* Input variables. */
	int32 w_id = params.w_id;
	int32 d_id = params.d_id;
	int32 c_id = params.c_id;
        int32 o_all_local = params.all_local_warehouse ? 1 : 0;  // NEED CHECK
	int32 o_ol_cnt = params.ol_cnt;

	int32 ol_i_id[15];
	int32 ol_supply_w_id[15];  // NEED CHECK
	int32 ol_quantity[15];

	std::int32_t i;

	double w_tax;

	double d_tax;
	int32 d_next_o_id;

	double c_discount;
        std::string c_last;
        std::string c_credit;

	float order_amount = 0.0;

	double i_price[15];
	std::string i_name[15];
	std::string i_data[15];

	double ol_amount[15];
	std::int32_t s_quantity[15];
	std::string my_s_dist[15];
	std::string s_data[15];

        if( d_id < 1 || 10 < d_id) {
            elog(ERROR, "d_id (%d) is out of valid range", d_id);
        }

	/* Loop through the last set of parameters. */
	for (i = 0; i < o_ol_cnt; i++) {
            ol_i_id[i] = params.itemid.at(i);
            ol_supply_w_id[i] = params.supply_wid; // differ from Foedus code.
            ol_quantity[i] = params.qty.at(i);
        }
        if (params.will_rollback) {
            ol_i_id[o_ol_cnt++] = 0;
        }

	elog(DEBUG1, "%d w_id = %d", (int) getpid(), w_id);
	elog(DEBUG1, "%d d_id = %d", (int) getpid(), d_id);
	elog(DEBUG1, "%d c_id = %d", (int) getpid(), c_id);
	elog(DEBUG1, "%d o_all_local = %d", (int) getpid(), o_all_local);
	elog(DEBUG1, "%d o_ol_cnt = %d", (int) getpid(), o_ol_cnt);

	elog(DEBUG1, "%d ##  ol_i_id  ol_supply_w_id  ol_quantity",
		(int) getpid());
	elog(DEBUG1, "%d --  -------  --------------  -----------",
		(int) getpid());
	for (i = 0; i < o_ol_cnt; i++) {
		elog(DEBUG1, "%d %2d  %7d  %14d  %11d", (int) getpid(),
				i + 1, ol_i_id[i], ol_supply_w_id[i], ol_quantity[i]);
	}

        if(connection->begin(transaction) != ERROR_CODE::OK) {
            elog(ERROR, "failed to begin");
        }

        NEW_ORDER_1->set_parameter(w_id);
        if(transaction->execute_query(NEW_ORDER_1, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //        if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //            elog(ERROR, "get_metadata failed");
        //        }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(w_tax) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            elog(DEBUG1, "%d w_tax = %f", (int) getpid(), w_tax);
        } else {
            elog(WARNING, "NEW_ORDER_1 failed");
            transaction->rollback();
            PG_RETURN_INT32(-10);
        }

        NEW_ORDER_2->set_parameter(w_id);
        NEW_ORDER_2->set_parameter(d_id);
        if(transaction->execute_query(NEW_ORDER_2, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //        if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //            elog(ERROR, "get_metadata failed");
        //        }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(d_tax) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            if(result_set->next_column(d_next_o_id) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
		elog(DEBUG1, "%d d_tax = %f", (int) getpid(), d_tax);
		elog(DEBUG1, "%d d_next_o_id = %d", (int) getpid(),
			d_next_o_id);
        } else {
                elog(WARNING, "NEW_ORDER_2 failed: %s", std::string(ogawayama::stub::error_name(err)).c_str());
                transaction->rollback();
		PG_RETURN_INT32(-11);
        }

        NEW_ORDER_3->set_parameter(w_id);
        NEW_ORDER_3->set_parameter(d_id);
        if(transaction->execute_statement(NEW_ORDER_3) != ERROR_CODE::OK) {
            elog(WARNING, "NEW_ORDER_3 failed");
            transaction->rollback();
            PG_RETURN_INT32(-12);
        }

        NEW_ORDER_4->set_parameter(w_id);
        NEW_ORDER_4->set_parameter(d_id);
        NEW_ORDER_4->set_parameter(c_id);
        if(transaction->execute_query(NEW_ORDER_4, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //        if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //            elog(ERROR, "get_metadata failed");
        //        }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(c_discount) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            if(result_set->next_column(c_last) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            if(result_set->next_column(c_credit) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            elog(DEBUG1, "%d c_discount = %f", (int) getpid(), c_discount);
            elog(DEBUG1, "%d c_last = %s", (int) getpid(), std::string(c_last).c_str());
            elog(DEBUG1, "%d c_credit = %s", (int) getpid(), std::string(c_credit).c_str());
        } else {
            elog(WARNING, "NEW_ORDER_4 failed");
            transaction->rollback();
            PG_RETURN_INT32(-13);
        }

        NEW_ORDER_5->set_parameter(d_next_o_id);
        NEW_ORDER_5->set_parameter(w_id);
        NEW_ORDER_5->set_parameter(d_id);
        if(transaction->execute_statement(NEW_ORDER_5) != ERROR_CODE::OK) {
            elog(WARNING, "NEW_ORDER_5 failed");
            transaction->rollback();
            PG_RETURN_INT32(-14);
        }

        NEW_ORDER_6->set_parameter(d_next_o_id);
        NEW_ORDER_6->set_parameter(d_id);
        NEW_ORDER_6->set_parameter(w_id);
        NEW_ORDER_6->set_parameter(c_id);
        NEW_ORDER_6->set_parameter(get_timestamp());
        NEW_ORDER_6->set_parameter(o_ol_cnt);
        NEW_ORDER_6->set_parameter(o_all_local);
        if(transaction->execute_statement(NEW_ORDER_6) != ERROR_CODE::OK) {
            elog(WARNING, "NEW_ORDER_6 failed");
            transaction->rollback();
            PG_RETURN_INT32(-15);
        }

        NEW_ORDER_61->set_parameter(d_id);
        NEW_ORDER_61->set_parameter(w_id);
        NEW_ORDER_61->set_parameter(c_id);
        NEW_ORDER_61->set_parameter(d_next_o_id);
        if(transaction->execute_statement(NEW_ORDER_61) != ERROR_CODE::OK) {
            elog(WARNING, "NEW_ORDER_61 failed");
            transaction->rollback();
            PG_RETURN_INT32(-155);
        }

	for (i = 0; i < o_ol_cnt; i++) {
                std::int32_t decr_quantity;
            
                NEW_ORDER_7->set_parameter(ol_i_id[i]);
                if(transaction->execute_query(NEW_ORDER_7, result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
		/*
		 * Shouldn't have to check if ol_i_id is 0, but if the row
		 * doesn't exist, the query still passes.
		 */
                err = result_set->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set->next_column(i_price[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    if(result_set->next_column(i_name[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    if(result_set->next_column(i_data[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    elog(DEBUG1, "%d i_price[%d] = %f", (int) getpid(), i,
                         i_price[i]);
                    elog(DEBUG1, "%d i_name[%d] = %s", (int) getpid(), i,
                         std::string(i_name[i]).c_str());
                    elog(DEBUG1, "%d i_data[%d] = %s", (int) getpid(), i,
                         std::string(i_data[i]).c_str());
                } else if(ol_i_id[i] == 0) {
                    transaction->rollback();
                    PG_RETURN_INT32(1);  // intentional rollback
                } else {
                    elog(WARNING, "NEW_ORDER_7 failed");
                    transaction->rollback();
                    PG_RETURN_INT32(-155);
                }

		ol_amount[i] = i_price[i] * (float) ol_quantity[i];

                NEW_ORDER_8x(d_id)->set_parameter(ol_i_id[i]);
                NEW_ORDER_8x(d_id)->set_parameter(w_id);
                if(transaction->execute_query(NEW_ORDER_8x(d_id), result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                err = result_set->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set->next_column(s_quantity[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    if(result_set->next_column(my_s_dist[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    if(result_set->next_column(s_data[i]) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");
                    }
                    elog(DEBUG1, "%d s_quantity[%d] = %d", (int) getpid(),
                         i, s_quantity[i]);
                    elog(DEBUG1, "%d my_s_dist[%d] = %s", (int) getpid(),
                         i, std::string(my_s_dist[i]).c_str());
                    elog(DEBUG1, "%d s_data[%d] = %s", (int) getpid(), i,
                         std::string(s_data[i]).c_str());
                    elog(INFO, "%d s_data[%d] = %s", (int) getpid(), i,
                         std::string(s_data[i]).c_str());

                } else {
                    transaction->rollback();
                    PG_RETURN_INT32(-16);
                }
		order_amount += ol_amount[i];

		if (s_quantity[i] > ol_quantity[i] + 10) {
			decr_quantity = ol_quantity[i];
		} else {
			decr_quantity = ol_quantity[i] - 91;
		}
                NEW_ORDER_9->set_parameter(decr_quantity);
                NEW_ORDER_9->set_parameter(ol_i_id[i]);
                NEW_ORDER_9->set_parameter(w_id);
                if(transaction->execute_statement(NEW_ORDER_9) != ERROR_CODE::OK) {
                    elog(WARNING, "NEW_ORDER_9 failed");
                    transaction->rollback();
                    PG_RETURN_INT32(-17);
                }

                NEW_ORDER_10->set_parameter(d_next_o_id);
                NEW_ORDER_10->set_parameter(d_id);
                NEW_ORDER_10->set_parameter(w_id);
                NEW_ORDER_10->set_parameter(i + 1);
                NEW_ORDER_10->set_parameter(ol_i_id[i]);
                NEW_ORDER_10->set_parameter(ol_supply_w_id[i]);
                NEW_ORDER_10->set_parameter(ol_quantity[i]);
                NEW_ORDER_10->set_parameter(ol_amount[i]);
                NEW_ORDER_10->set_parameter(my_s_dist[i]);
                if(transaction->execute_statement(NEW_ORDER_10) != ERROR_CODE::OK) {
                    elog(WARNING, "NEW_ORDER_10 failed");
                    transaction->rollback();
                    PG_RETURN_INT32(-18);
                }
	}

        //	SPI_finish();
        transaction->commit();
	PG_RETURN_INT32(0);
}

}  // namespace tpcc
}  // namespace ogawayama
