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

#include "glog/logging.h"

#include "ogawayama/stub/api.h"

#include "tpcc_common.h"
#include "tpcc_transaction.h"


namespace ogawayama {
namespace tpcc {

#define ORDER_STATUS_1 statements->get_preprad_statement(0)
#define ORDER_STATUS_2 statements->get_preprad_statement(1)
#define ORDER_STATUS_3 statements->get_preprad_statement(2)
#define ORDER_STATUS_4 statements->get_preprad_statement(3)
#define ORDER_STATUS_11 statements->get_preprad_statement(4)
#define ORDER_STATUS_12 statements->get_preprad_statement(5)
#define ORDER_STATUS_31 statements->get_preprad_statement(6)


cached_statement order_status_statements[] = {
	{ /* ORDER_STATUS_1 */
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = :p1\n" \
	"  AND c_d_id = :p2\n" \
	"  AND c_last = :p3\n" \
	"ORDER BY c_first ASC",
	},

	{ /* ORDER_STATUS_2 */
	"SELECT c_first, c_middle, c_last, c_balance\n" \
	"FROM customer\n" \
	"WHERE c_w_id = :p1\n" \
	"  AND c_d_id = :p2\n" \
	"  AND c_id = :p3",
	},

	{ /* ORDER_STATUS_3 */
	"SELECT o_carrier_id, o_entry_d, o_ol_cnt\n" \
	"FROM orders\n" \
	"WHERE o_w_id = :p1\n" \
	"  AND o_d_id = :p2\n" \
	"  AND o_id = :p3\n",
	},

	{ /* ORDER_STATUS_4 */
	"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,\n" \
	"	ol_delivery_d\n" \
	"FROM order_line\n" \
	"WHERE ol_w_id = :p1\n" \
	"  AND ol_d_id = :p2\n" \
	"  AND ol_o_id = :p3",
	},

	{ /* ORDER_STATUS_11 */        
            "SELECT COUNT(c_id) FROM customer_secondary "
            "WHERE "
            "c_w_id = :p1 AND "
            "c_d_id = :p2 AND "
            "c_last = :p3",
        },

	{ /* ORDER_STATUS_12 */
            "SELECT c_id FROM customer_secondary "
            "WHERE "
            "c_w_id = :p1 AND "
            "c_d_id = :p2 AND "
            "c_last = :p3"
            " ORDER by c_first ",
        },
        
	{ /* ORDER_STATUS_31 */
            "SELECT o_id FROM orders_secondary "
            "WHERE "
            "o_w_id = :p2 AND "
            "o_d_id = :p1 AND "
            "o_c_id = :p3"
            " ORDER by o_id DESC",
        },
        
	{ NULL }
};


/* Prototypes to prevent potential gcc warnings. */

int
transaction_orderstatus(ConnectionPtr::element_type *connection, prepared_statements *statements, randomGeneratorClass *randomGenerator, std::uint16_t warehouse_low, std::uint16_t warehouse_high, [[maybe_unused]] tpcc_profiler *profiler)
{
    orderstatusParams params = {};
    params.w_id = randomGenerator->uniformWithin(warehouse_low, warehouse_high);
    params.d_id = randomGenerator->uniformWithin(1, scale::districts);
    params.byname = randomGenerator->uniformWithin(1, 100) <= 60;
    if (params.byname) {
        Lastname(randomGenerator->nonUniformWithin(255, 0, scale::lnames - 1), static_cast<char *>(params.c_last));
    } else {
        params.c_id = randomGenerator->nonUniformWithin(1023, 1, scale::customers);
    }

        TransactionPtr transaction;
        ResultSetPtr result_set;
        ResultSetPtr result_set_tmp{};
        MetadataPtr metadata;
        ERROR_CODE err;

        //        /* for Set Returning Function (SRF) */
        //	FuncCallContext  *funcctx;
        //	MemoryContext     oldcontext;

        //	if (SRF_IS_FIRSTCALL()) {

		/* Input variables. */
                int32 c_id = params.byname ? 0: params.c_id;
		int32 c_w_id = params.w_id;
		int32 c_d_id = params.d_id;
                std::string c_last(params.c_last);

		/* temp variables */
		std::int32_t count;

		std::int32_t my_c_id = 0;

		std::string c_first;
		std::string c_middle;
		std::string my_c_last;
		double c_balance;

		std::int32_t o_id;
		std::int32_t o_carrier_id;
		std::string o_entry_d;
		std::int32_t o_ol_cnt;

		/* SRF setup */
                //		funcctx = SRF_FIRSTCALL_INIT();

                if(connection->begin(transaction) != ERROR_CODE::OK) {
                    elog(ERROR, "failed to begin");
                }

		/*
		 * switch into the multi_call_memory_ctx, anything that is
		 * palloc'ed will be preserved across calls .
		 */
                //		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (c_id == 0) {
                    ORDER_STATUS_11->set_parameter(c_w_id);
                    ORDER_STATUS_11->set_parameter(c_d_id);
                    ORDER_STATUS_11->set_parameter(c_last);
                    if(transaction->execute_query(ORDER_STATUS_11, result_set_tmp) != ERROR_CODE::OK) {
                        elog(ERROR, "execute_query failed");
                    }
                    //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                    //                    elog(ERROR, "get_metadata failed");
                    //                }
                    //                auto& md = metadata->get_types();

                    err = result_set_tmp->next();
                    if(err == ERROR_CODE::OK) {
                        if(result_set_tmp->next_column(count) != ERROR_CODE::OK) {
                            elog(ERROR, "next_column failed (count)");
                        }
                        elog(DEBUG1, "%d total", count);
                    } else {
                        //                        SRF_RETURN_DONE(funcctx);
                        return -1;  // Error
                    }

                    if(count == 0) {
                        //                        SRF_RETURN_DONE(funcctx);
                        return 0;  // OK
                    }
                    result_set_tmp = nullptr;

                    ORDER_STATUS_12->set_parameter(c_w_id);
                    ORDER_STATUS_12->set_parameter(c_d_id);
                    ORDER_STATUS_12->set_parameter(c_last);
                    if(transaction->execute_query(ORDER_STATUS_12, result_set_tmp) != ERROR_CODE::OK) {
                        elog(ERROR, "execute_query failed");
                    }
                    //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                    //                    elog(ERROR, "get_metadata failed");
                    //                }
                    //                auto& md = metadata->get_types();
                    if (count%2 > 0) { count++; }
                    for(int n=0; n<count/2; n++) {
                        if(result_set_tmp->next() != ERROR_CODE::OK) {
                            elog(ERROR, "next failed");
                            //                            SRF_RETURN_DONE(funcctx);
                            return -1;  // Error
                        }
                    }
                    if(result_set_tmp->next_column(my_c_id) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (my_c_id)");
                    }
                    elog(DEBUG1, "c_id = %d", my_c_id);
		} else {
			my_c_id = c_id;
		}
                result_set_tmp = nullptr;

                ORDER_STATUS_2->set_parameter(c_w_id);
                ORDER_STATUS_2->set_parameter(c_d_id);
                ORDER_STATUS_2->set_parameter(my_c_id);
                if(transaction->execute_query(ORDER_STATUS_2, result_set_tmp) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set_tmp->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set_tmp->next_column(c_first) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (c_first)");
                    }  
                    if(result_set_tmp->next_column(c_middle) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (c_middle");
                    }
                    if(result_set_tmp->next_column(my_c_last) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (my_c_last)");
                    }
                    if(result_set_tmp->next_column(c_balance) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (c_balance)");
                    }
                    elog(DEBUG1, "c_first = %s", std::string(c_first).c_str());
                    elog(DEBUG1, "c_middle = %s", std::string(c_middle).c_str());
                    elog(DEBUG1, "c_last = %s", std::string(my_c_last).c_str());
                    elog(DEBUG1, "c_balance = %f", c_balance);
                } else {
                    //                    SRF_RETURN_DONE(funcctx);
                    return -1;  // Error
                }
                result_set_tmp = nullptr;


                ORDER_STATUS_31->set_parameter(c_w_id);
                ORDER_STATUS_31->set_parameter(c_d_id);
                ORDER_STATUS_31->set_parameter(my_c_id);
                if(transaction->execute_query(ORDER_STATUS_31, result_set_tmp) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set_tmp->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set_tmp->next_column(o_id) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (o_id)");
                    }
                    elog(DEBUG1, "o_id = %d", o_id);
                } else {
                    //                    SRF_RETURN_DONE(funcctx);
                    return -1;  // Error
                }
                result_set_tmp = nullptr;

                ORDER_STATUS_3->set_parameter(c_w_id);
                ORDER_STATUS_3->set_parameter(c_d_id);
                ORDER_STATUS_3->set_parameter(o_id);
                if(transaction->execute_query(ORDER_STATUS_3, result_set_tmp) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set_tmp->next();
                if(err == ERROR_CODE::OK) {
                    err = result_set_tmp->next_column(o_carrier_id);
                    if(err != ERROR_CODE::OK) {
                        if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            o_carrier_id = -1;  // I don't know the right wey to treat this case.
                        } else {
                            elog(ERROR, "next_column failed (o_carrier_id)");
                        }
                    }
                    err = result_set_tmp->next_column(o_entry_d);
                    if(err != ERROR_CODE::OK) {
                        if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            o_entry_d = "null";
                        } else {
                            elog(ERROR, "next_column failed (o_entry_d)");
                        }
                    }
                    if(result_set_tmp->next_column(o_ol_cnt) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed (o_ol_cnt)");
                    }
                    elog(DEBUG1, "o_carrier_id = %d", o_carrier_id);
                    elog(DEBUG1, "o_entry_d = %s", std::string(o_entry_d).c_str());
                    elog(DEBUG1, "o_ol_cnt = %d", o_ol_cnt);
                } else {
                    //                    SRF_RETURN_DONE(funcctx);
                    return -1;  // Error
                }
                result_set_tmp = nullptr;

                ORDER_STATUS_4->set_parameter(c_w_id);
                ORDER_STATUS_4->set_parameter(c_d_id);
                ORDER_STATUS_4->set_parameter(o_id);
                if(transaction->execute_query(ORDER_STATUS_4, result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                    elog(ERROR, "get_metadata failed");
                }
                auto& md = metadata->get_types();
		count = md.size();

		elog(DEBUG1, "##  ol_i_id  ol_supply_w_id  ol_quantity  ol_amount  ol_delivery_d");
		elog(DEBUG1, "--  -------  --------------  -----------  ---------  -------------");

#if 0
                /* get tupdesc from the type name */
                TupleDesc tupdesc = RelationNameGetTupleDesc("status_info");

                /*
                 * generate attribute metadata needed later to produce tuples
                 * from raw C strings
                 */
                funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

                /* total number of tuples to be returned */
		funcctx->max_calls = count;

                //		/* switch out of the multi_call_memory_ctx */
                //		MemoryContextSwitchTo(oldcontext);
                //	}

                //	/* SRF setup */
                //	funcctx = SRF_PERCALL_SETUP();

                //	/* test if we are done */
                //	if (funcctx->call_cntr < funcctx->max_calls) {
                //		/* Here we want to return another item: */
#endif

		/* setup some variables */
                //		Datum result;
                const char* cstr_values[5];
                //                HeapTuple result_tuple;

                std::int32_t ol_i_id;
                std::int32_t ol_supply_w_id;
                std::int32_t ol_quantity;
                double ol_amount;
                std::string ol_delivery_d;
                std::string ol_delivery_d_str;

                while (true) {
                    err = result_set->next();
                    if(err == ERROR_CODE::OK) {
                        std::size_t j = 0;
                        //                        cstr_values = (const char **) palloc(5 * sizeof(char *));

                        err = result_set->next_column(ol_i_id);
                        if(err == ERROR_CODE::OK) {
                            cstr_values[j] = std::to_string(ol_i_id).c_str();
                        } else if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            cstr_values[j] = "null";
                        } else {
                            elog(ERROR, "next_column failed (ol_i_id)");
                        }
                        j++;

                        err = result_set->next_column(ol_supply_w_id);
                        if(err == ERROR_CODE::OK) {
                            cstr_values[j] = std::to_string(ol_supply_w_id).c_str();
                        } else if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            cstr_values[j] = "null";
                        } else {
                            elog(ERROR, "next_column failed (ol_supply_w_id)");
                        }
                        j++;

                        err = result_set->next_column(ol_quantity);
                        if(err == ERROR_CODE::OK) {
                            cstr_values[j] = std::to_string(ol_quantity).c_str();
                        } else if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            cstr_values[j] = "null";
                        } else {
                            elog(ERROR, "next_column failed (ol_quantity)");
                        }
                        j++;

                        err = result_set->next_column(ol_amount);
                        if(err == ERROR_CODE::OK) {
                            cstr_values[j] = std::to_string(ol_i_id).c_str();
                        } else if(err == ERROR_CODE::COLUMN_WAS_NULL) {
                            cstr_values[j] = "null";
                        } else {
                            elog(ERROR, "next_column failed (ol_i_id)");
                        }
                        j++;

                        ol_delivery_d_str = ol_delivery_d;
                        cstr_values[j] = ol_delivery_d_str.c_str();
                        elog(DEBUG1, "%2d  %7d  %14d  %11d  %9.2f  %13s",
                             static_cast<std::int32_t>(funcctx->call_cntr),
                             ol_i_id, ol_supply_w_id,
                             ol_quantity, ol_amount,
                             ol_delivery_d_str.c_str());
#if 0
                        /* build a tuple */
                        result_tuple = BuildTupleFromCStrings(funcctx->attinmeta, const_cast<char**>(cstr_values));

                        /* make the tuple into a datum */
                        result = HeapTupleGetDatum(result_tuple);
                        //                        SRF_RETURN_NEXT(funcctx, result);
#endif
                        dmy_use(cstr_values);
                    }
                    else if(err == ERROR_CODE::END_OF_ROW) {
                        //                        SRF_RETURN_DONE(funcctx);
                        break;
                    }
                }
                if(transaction->commit() != ERROR_CODE::OK) {
                    elog(ERROR, "commit failed");
                }
            //            transaction = nullptr;
            /* Here we are done returning items and just need to clean up: */
            //		SPI_finish();
            //            SRF_RETURN_DONE(funcctx);
                return 0;  // OK
}

void dmy_use([[maybe_unused]]const char *pp[]) {}

} //  namespace tpcc
} //  namespace ogawayama
