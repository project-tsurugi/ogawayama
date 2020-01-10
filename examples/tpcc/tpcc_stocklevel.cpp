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

#include <array>
#include <set>

#include "glog/logging.h"

#include "ogawayama/stub/api.h"

#include "tpcc_common.h"
#include "tpcc_transaction.h"


namespace ogawayama {
namespace tpcc {

#define STOCK_LEVEL_1 statements->get_preprad_statement(0)
#define STOCK_LEVEL_2 statements->get_preprad_statement(1)

cached_statement stocklevel_statements[] = {
	{ /* STOCK_LEVEL_1 */
	"SELECT d_next_o_id\n" \
	"FROM district\n" \
	"WHERE d_w_id = :p1\n" \
	"AND d_id = :p2",
	},

	{ /*1 STOCK_LEVEL_2 */
	"SELECT COUNT(DISTINCT s_i_id)\n" \
        "FROM order_line join stock ON s_i_id = ol_i_id\n" \
        "WHERE ol_w_id = :p1\n" \
        "  AND ol_d_id = :p2\n" \
        "  AND ol_o_id < :p3 - 1\n" \
        "  AND ol_o_id >= :p3 - 20\n" \
        "  AND s_w_id = :p1\n" \
        "  AND s_quantity < :p4",
	},

	{ NULL }
};

int
transaction_stocklevel(ConnectionPtr::element_type *connection, prepared_statements *statements, randomGeneratorClass *randomGenerator, std::uint16_t warehouse_low, std::uint16_t warehouse_high, [[maybe_unused]] tpcc_profiler *profiler)
{
    stocklevelParams params = {};
    
    params.w_id = randomGenerator->uniformWithin(warehouse_low, warehouse_high);
    params.d_id = randomGenerator->uniformWithin(1, scale::districts);
    params.threshold = randomGenerator->uniformWithin(10, 20);

        TransactionPtr transaction;
        ResultSetPtr result_set{};
        //        MetadataPtr metadata;
        ERROR_CODE err;

	/* Input variables. */
	int32 w_id = params.w_id;
	int32 d_id = params.d_id;
        int32 threshold = params.threshold;

	std::int32_t d_next_o_id;
	std::int32_t low_stock;

        if(connection->begin(transaction) != ERROR_CODE::OK) {
            elog(ERROR, "failed to begin");
        }

        STOCK_LEVEL_1->set_parameter(w_id);
        STOCK_LEVEL_1->set_parameter(d_id);
        if(transaction->execute_query(STOCK_LEVEL_1, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //        if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //            elog(ERROR, "get_metadata failed");
        //        }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(d_next_o_id) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            elog(DEBUG1, "d_next_o_id = %d", d_next_o_id);
        } else {
            elog(WARNING, "STOCK_LEVEL_1 failed");
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        STOCK_LEVEL_2->set_parameter(w_id);
        STOCK_LEVEL_2->set_parameter(d_id);
        STOCK_LEVEL_2->set_parameter(d_next_o_id);
        STOCK_LEVEL_2->set_parameter(threshold);
        if(transaction->execute_query(STOCK_LEVEL_2, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //        if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //            elog(ERROR, "get_metadata failed");
        //        }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(low_stock) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            elog(DEBUG1, "low_stock = %d", low_stock);
        } else {
            elog(WARNING, "STOCK_LEVEL_2 failed");
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        //	SPI_finish();
        transaction->commit();
	PG_RETURN_INT32(0);  // OK
}

} //  namespace tpcc
} //  namespace ogawayama
