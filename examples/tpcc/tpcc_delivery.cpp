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

/*
 * Delivery transaction SQL statements.
 */

#define DELIVERY_1 statements->get_preprad_statement(0)
#define DELIVERY_2 statements->get_preprad_statement(1)
#define DELIVERY_3 statements->get_preprad_statement(2)
#define DELIVERY_4 statements->get_preprad_statement(3)
#define DELIVERY_5 statements->get_preprad_statement(4)
#define DELIVERY_6 statements->get_preprad_statement(5)
#define DELIVERY_7 statements->get_preprad_statement(6)

cached_statement delivery_statements[] =
{
	/* DELIVERY_1 */
	{
	"SELECT no_o_id\n" \
	"FROM new_order\n" \
	"WHERE no_w_id = :p1\n" \
	"  AND no_d_id = :p2\n" \
	"ORDER BY no_o_id ",
	},

	/* DELIVERY_2 */
	{
	"DELETE FROM new_order\n" \
	"WHERE no_o_id = :p1\n" \
	"  AND no_w_id = :p2\n" \
	"  AND no_d_id = :p3",
	},

	/* DELIVERY_3 */
	{
	"SELECT o_c_id\n" \
	"FROM orders\n" \
	"WHERE o_id = :p1\n" \
	"  AND o_w_id = :p2\n" \
	"  AND o_d_id = :p3",
	},

	/* DELIVERY_4 */
	{
	"UPDATE orders\n" \
	"SET o_carrier_id = :p1\n" \
	"WHERE o_id = :p2\n" \
	"  AND o_w_id = :p3\n" \
	"  AND o_d_id = :p4",
	},

	/* DELIVERY_5 */
	{
	"UPDATE order_line\n" \
	"SET ol_delivery_d = :p4\n" \
	"WHERE ol_o_id = :p1\n" \
	"  AND ol_w_id = :p2\n" \
	"  AND ol_d_id = :p3",
        // :p4 = current_timestamp
	},

	/* DELIVERY_6 */
	{
	"SELECT SUM(ol_amount)\n" \
	"FROM order_line\n" \
	"WHERE ol_o_id = :p1\n" \
	"  AND ol_w_id = :p2\n" \
	"  AND ol_d_id = :p3",
	},

	/* DELIVERY_7 */
	{
	"UPDATE customer\n" \
	"SET c_balance = c_balance + :p1\n" \
	"WHERE c_id = :p2\n" \
	"  AND c_w_id = :p3\n" \
	"  AND c_d_id = :p4",
	},

	{ NULL }
};


int
transaction_delivery(ConnectionPtr::element_type *connection, prepared_statements *statements, randomGeneratorClass *randomGenerator, std::uint16_t warehouse_low, std::uint16_t warehouse_high, [[maybe_unused]] tpcc_profiler *profiler)
{
    deliveryParams params = {};
    params.w_id = randomGenerator->uniformWithin(warehouse_low, warehouse_high);
    params.o_carrier_id = randomGenerator->uniformWithin(1, 10);

    gettimestamp(static_cast<char *>(params.ol_delivery_d));

        TransactionPtr transaction;
        ResultSetPtr result_set{};
        //        MetadataPtr metadata;
        ERROR_CODE err;

	/* Input variables. */
	int32 w_id = params.w_id;
	int32 o_carrier_id = params.o_carrier_id;

	std::int32_t d_id;
        double ol_amount;
	std::int32_t no_o_id;
	std::int32_t o_c_id;

        if(connection->begin(transaction) != ERROR_CODE::OK) {
            elog(ERROR, "failed to begin");
        }

	for (d_id = 1; d_id <= 10; d_id++) {
                DELIVERY_1->set_parameter(w_id);
		DELIVERY_1->set_parameter(d_id);
                if(transaction->execute_query(DELIVERY_1, result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set->next_column(no_o_id) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");                        
                    }
                    elog(DEBUG1, "no_o_id = %d", no_o_id);
                } else if (err == ERROR_CODE::END_OF_ROW) {
                    /* Nothing to deliver for this district, try next district. */
                    continue;
                }

                
                DELIVERY_2->set_parameter(no_o_id);
		DELIVERY_2->set_parameter(w_id);
                DELIVERY_2->set_parameter(d_id);
                if(transaction->execute_statement(DELIVERY_2) != ERROR_CODE::OK) {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
		}

                DELIVERY_3->set_parameter(no_o_id);
		DELIVERY_3->set_parameter(w_id);
                DELIVERY_3->set_parameter(d_id);
                if(transaction->execute_query(DELIVERY_3, result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set->next_column(o_c_id) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");                        
                    }
                    elog(DEBUG1, "o_c_id = %d", o_c_id);
                } else {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
                }
                
                DELIVERY_4->set_parameter(o_carrier_id);
		DELIVERY_4->set_parameter(no_o_id);
                DELIVERY_4->set_parameter(w_id);
                DELIVERY_4->set_parameter(d_id);
                if(transaction->execute_statement(DELIVERY_4) != ERROR_CODE::OK) {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
		}

		DELIVERY_5->set_parameter(no_o_id);
                DELIVERY_5->set_parameter(w_id);
                DELIVERY_5->set_parameter(d_id);
                DELIVERY_5->set_parameter(get_timestamp());
                if(transaction->execute_statement(DELIVERY_5) != ERROR_CODE::OK) {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
		}

                DELIVERY_6->set_parameter(no_o_id);
		DELIVERY_6->set_parameter(w_id);
                DELIVERY_6->set_parameter(d_id);
                if(transaction->execute_query(DELIVERY_6, result_set) != ERROR_CODE::OK) {
                    elog(ERROR, "execute_query failed");
                }
                //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
                //                    elog(ERROR, "get_metadata failed");
                //                }
                //                auto& md = metadata->get_types();

                err = result_set->next();
                if(err == ERROR_CODE::OK) {
                    if(result_set->next_column(ol_amount) != ERROR_CODE::OK) {
                        elog(ERROR, "next_column failed");                        
                    }
                    elog(DEBUG1, "o_c_id = %d", o_c_id);
                } else {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
                }

                DELIVERY_7->set_parameter(ol_amount);
                DELIVERY_7->set_parameter(o_c_id);
		DELIVERY_7->set_parameter(w_id);
                DELIVERY_7->set_parameter(d_id);
                if(transaction->execute_statement(DELIVERY_7) != ERROR_CODE::OK) {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
		}
	}

        //        SPI_finish();
        transaction->commit();
	PG_RETURN_INT32(0);  // OK
}
    
} //  namespace tpcc
} //  namespace ogawayama
