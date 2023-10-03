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

#include <string>

#include "glog/logging.h"

#include "ogawayama/stub/api.h"

#include "tpcc_common.h"
#include "tpcc_transaction.h"


namespace ogawayama {
namespace tpcc {

#define PAYMENT_1 statements->get_preprad_statement(0)
#define PAYMENT_2 statements->get_preprad_statement(1)
#define PAYMENT_3 statements->get_preprad_statement(2)
#define PAYMENT_4 statements->get_preprad_statement(3)
#define PAYMENT_5 statements->get_preprad_statement(4)
#define PAYMENT_6 statements->get_preprad_statement(5)
#define PAYMENT_7_GC statements->get_preprad_statement(6)
#define PAYMENT_7_BC statements->get_preprad_statement(7)
// #define PAYMENT_8 statements->get_preprad_statement(8)
#define PAYMENT_51 statements->get_preprad_statement(8)
#define PAYMENT_52 statements->get_preprad_statement(9)

cached_statement payment_statements[] =
{
	{ /* PAYMENT_1 */
	"SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip\n" \
	"FROM warehouse\n" \
	"WHERE w_id = :p1",
	},

	{ /* PAYMENT_2 */
	"UPDATE warehouse\n" \
	"SET w_ytd = w_ytd + :p1\n" \
	"WHERE w_id = :p2",
	},

	{ /* PAYMENT_3 */
	"SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip\n" \
	"FROM district\n" \
	"WHERE d_id = :p1\n" \
	"  AND d_w_id = :p2",
	},

	{ /* PAYMENT_4 */
	"UPDATE district\n" \
	"SET d_ytd = d_ytd + :p1\n" \
	"WHERE d_id = :p2\n" \
	"  AND d_w_id = :p3",
	},

	{ /* PAYMENT_5 */
	"SELECT c_id\n" \
	"FROM customer\n" \
	"WHERE c_w_id = :p1\n" \
	"  AND c_d_id = :p2\n" \
	"  AND c_last = :p3\n" \
	"ORDER BY c_first ASC",
	},

	{ /* PAYMENT_6 */
	"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city,\n" \
	"       c_state, c_zip, c_phone, c_since, c_credit,\n" \
	"       c_credit_lim, c_discount, c_balance, c_data, c_ytd_payment\n" \
	"FROM customer\n" \
	"WHERE c_w_id = :p1\n" \
	"  AND c_d_id = :p2\n" \
	"  AND c_id = :p3",
	},

	{ /* PAYMENT_7_GC */
	"UPDATE customer\n" \
	"SET c_balance = c_balance - :p1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1\n" \
	"WHERE c_id = :p2\n" \
	"  AND c_w_id = :p3\n" \
	"  AND c_d_id = :p4",
	},

	{ /* PAYMENT_7_BC */
	"UPDATE customer\n" \
	"SET c_balance = c_balance - :p1,\n" \
	"    c_ytd_payment = c_ytd_payment + 1,\n" \
//	"    c_data = substring(:p2 || c_data, 1 , 500)\n"
        "    c_data = :p2 \n" \
	"WHERE c_id = :p3\n" \
	"  AND c_w_id = :p4\n" \
	"  AND c_d_id = :p5",
	},
#if 0
        { /* PAYMENT_8 */
        "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id,\n" \
        "		     h_date, h_amount, h_data)\n" \
        "VALUES (:p1, :p2, :p3, :p4, :p5, :p8, :p6, :p7 || '    ' || :p8)",
        // p8: = current_timestamp
        },
#endif
	{ /* PAYMENT_51 */        
            "SELECT COUNT(c_id) FROM customer_secondary "
            "WHERE "
            "c_w_id = :p1 AND "
            "c_d_id = :p2 AND "
            "c_last = :p3",
        },

	{ /* PAYMENT_52 */
            "SELECT c_id FROM customer_secondary "
            "WHERE "
            "c_w_id = :p1 AND "
            "c_d_id = :p2 AND "
            "c_last = :p3"
            " ORDER by c_first ",
        },

	{ NULL }
};

int
transaction_payment(ConnectionPtr::element_type *connection, prepared_statements *statements, randomGeneratorClass *randomGenerator, std::uint16_t warehouse_low, std::uint16_t warehouse_high, [[maybe_unused]] tpcc_profiler *profiler)
{
    paymentParams params = {};
    params.w_id = randomGenerator->uniformWithin(warehouse_low, warehouse_high);
    params.d_id = randomGenerator->uniformWithin(1, scale::districts);
    params.h_amount = static_cast<double>(randomGenerator->uniformWithin(100, 500000)) / 100.0f;
    params.byname = randomGenerator->uniformWithin(1, 100) <= 60;
    if (params.byname) {
        Lastname(randomGenerator->nonUniformWithin(255, 0, scale::lnames - 1), static_cast<char *>(params.c_last));
    } else {
        params.c_id = randomGenerator->nonUniformWithin(1023, 1, scale::customers);
    }
    getdatestamp(static_cast<char *>(params.h_date));
    randomGenerator->MakeAlphaString(12,24,static_cast<char *>(params.h_data));
    
        TransactionPtr transaction{};
        ResultSetPtr result_set{};
        //        MetadataPtr metadata;
        ERROR_CODE err;

	/* Input variables. */
	int32 w_id = params.w_id;
	int32 d_id = params.d_id;
	int32 c_id = params.byname ? 0 : params.c_id;
	int32 c_w_id = params.w_id;
	int32 c_d_id = params.d_id;
        std::string c_last(params.c_last);
	float4 h_amount = params.h_amount;

	std::string w_name;
	std::string w_street_1;
	std::string w_street_2;
	std::string w_city;
	std::string w_state;
	std::string w_zip;

	std::string d_name;
	std::string d_street_1;
	std::string d_street_2;
	std::string d_city;
	std::string d_state;
	std::string d_zip;

	std::int32_t my_c_id = 0;
	std::int32_t count;

	std::string c_first;
	std::string c_middle;
	std::string my_c_last;
	std::string c_street_1;
	std::string c_street_2;
	std::string c_city;
	std::string c_state;
	std::string c_zip;
	std::string c_phone;
	std::string c_since;
	std::string c_credit;
	double c_credit_lim;
	double c_discount;
	double c_balance;
	std::string c_data;
	double c_ytd_payment;

	elog(DEBUG1, "w_id = %d", w_id);
	elog(DEBUG1, "d_id = %d", d_id);
	elog(DEBUG1, "c_id = %d", c_id);
	elog(DEBUG1, "c_w_id = %d", c_w_id);
	elog(DEBUG1, "c_d_id = %d", c_d_id);
	elog(DEBUG1, "c_last = %s", std::string(c_last).c_str());
	elog(DEBUG1, "h_amount = %f", h_amount);

        if(connection->begin(transaction) != ERROR_CODE::OK) {
            elog(ERROR, "failed to begin");
        }

        PAYMENT_1->set_parameter(w_id);
        if(transaction->execute_query(PAYMENT_1, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //                    elog(ERROR, "get_metadata failed");
        //                }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(w_name) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(w_street_1) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(w_street_2) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(w_city) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(w_state) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(w_zip) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            elog(DEBUG1, "w_name = %s", std::string(w_name).c_str());
            elog(DEBUG1, "w_street_1 = %s", std::string(w_street_1).c_str());
            elog(DEBUG1, "w_street_2 = %s", std::string(w_street_2).c_str());
            elog(DEBUG1, "w_city = %s", std::string(w_city).c_str());
            elog(DEBUG1, "w_state = %s", std::string(w_state).c_str());
            elog(DEBUG1, "w_zip = %s", std::string(w_zip).c_str());
        } else {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        PAYMENT_2->set_parameter(h_amount);
        PAYMENT_2->set_parameter(w_id);
        if(transaction->execute_statement(PAYMENT_2) != ERROR_CODE::OK) {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        PAYMENT_3->set_parameter(d_id);
        PAYMENT_3->set_parameter(w_id);
        if(transaction->execute_query(PAYMENT_3, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //                    elog(ERROR, "get_metadata failed");
        //                }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(d_name) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(d_street_1) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(d_street_2) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(d_city) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(d_state) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(d_zip) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            elog(DEBUG1, "d_name = %s", std::string(d_name).c_str());
            elog(DEBUG1, "d_street_1 = %s", std::string(d_street_1).c_str());
            elog(DEBUG1, "d_street_2 = %s", std::string(d_street_2).c_str());
            elog(DEBUG1, "d_city = %s", std::string(d_city).c_str());
            elog(DEBUG1, "d_state = %s", std::string(d_state).c_str());
            elog(DEBUG1, "d_zip = %s", std::string(d_zip).c_str());
        } else {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        PAYMENT_4->set_parameter(h_amount);
        PAYMENT_4->set_parameter(d_id);
        PAYMENT_4->set_parameter(w_id);
        if(transaction->execute_statement(PAYMENT_4) != ERROR_CODE::OK) {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

        if (c_id == 0) {
            PAYMENT_51->set_parameter(c_w_id);
            PAYMENT_51->set_parameter(c_d_id);
            PAYMENT_51->set_parameter(c_last);
            if(transaction->execute_query(PAYMENT_51, result_set) != ERROR_CODE::OK) {
                elog(ERROR, "execute_query failed");
            }
            //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
            //                    elog(ERROR, "get_metadata failed");
            //                }
            //                auto& md = metadata->get_types();
            
            err = result_set->next();
            if(err == ERROR_CODE::OK) {
                if(result_set->next_column(count) != ERROR_CODE::OK) {
                    elog(ERROR, "next_column failed");
                }
                elog(DEBUG1, "%d total", count);
            } else {
                transaction->rollback();
                PG_RETURN_INT32(-1);
            }
            
            if(count == 0) {
                transaction->commit();
                PG_RETURN_INT32(0);  // OK
            }

            PAYMENT_52->set_parameter(c_w_id);
            PAYMENT_52->set_parameter(c_d_id);
            PAYMENT_52->set_parameter(c_last);
            if(transaction->execute_query(PAYMENT_52, result_set) != ERROR_CODE::OK) {
                elog(ERROR, "execute_query failed");
            }
            //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
            //                    elog(ERROR, "get_metadata failed");
            //                }
            //                auto& md = metadata->get_types();
            if (count%2 > 0) { count++; }
            for(int n=0; n<count/2; n++) {
                err = result_set->next();
                if(err != ERROR_CODE::OK) {
                    elog(ERROR, "next failed: %d %d %s", count, n, std::string(ogawayama::stub::error_name(err)).c_str());
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
                }
            }
            if(result_set->next_column(my_c_id) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");
            }
            elog(DEBUG1, "c_id = %d", my_c_id);
	} else {
            my_c_id = c_id;
	}

        PAYMENT_6->set_parameter(c_w_id);
        PAYMENT_6->set_parameter(c_d_id);
        PAYMENT_6->set_parameter(my_c_id);
        if(transaction->execute_query(PAYMENT_6, result_set) != ERROR_CODE::OK) {
            elog(ERROR, "execute_query failed");
        }
        //                if(result_set->get_metadata(metadata) != ERROR_CODE::OK) {
        //                    elog(ERROR, "get_metadata failed");
        //                }
        //                auto& md = metadata->get_types();

        err = result_set->next();
        if(err == ERROR_CODE::OK) {
            if(result_set->next_column(c_first) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_middle) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(my_c_last) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_street_1) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_street_2) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_city) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_state) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_zip) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_phone) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_since) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_credit) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_credit_lim) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_discount) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_balance) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_data) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            if(result_set->next_column(c_ytd_payment) != ERROR_CODE::OK) {
                elog(ERROR, "next_column failed");                        
            }
            elog(DEBUG1, "c_first = %s", std::string(c_first).c_str());
            elog(DEBUG1, "c_middle = %s", std::string(c_middle).c_str());
            elog(DEBUG1, "c_last = %s", std::string(my_c_last).c_str());
            elog(DEBUG1, "c_street_1 = %s", std::string(c_street_1).c_str());
            elog(DEBUG1, "c_street_2 = %s", std::string(c_street_2).c_str());
            elog(DEBUG1, "c_city = %s", std::string(c_city).c_str());
            elog(DEBUG1, "c_state = %s", std::string(c_state).c_str());
            elog(DEBUG1, "c_zip = %s", std::string(c_zip).c_str());
            elog(DEBUG1, "c_phone = %s", std::string(c_phone).c_str());
            elog(DEBUG1, "c_since = %s", std::string(c_since).c_str());
            elog(DEBUG1, "c_credit = %s", std::string(c_credit).c_str());
            elog(DEBUG1, "c_credit_lim = %f", c_credit_lim);
            elog(DEBUG1, "c_discount = %f", c_discount);
            elog(DEBUG1, "c_balance = %f", c_balance);
            elog(DEBUG1, "c_data = %s", std::string(c_data).c_str());
            elog(DEBUG1, "c_ytd_payment = %f", c_ytd_payment);
        } else {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }

	/* It's either "BC" or "GC". */
	if (c_credit[0] == 'G') {
            PAYMENT_7_GC->set_parameter(h_amount);
            PAYMENT_7_GC->set_parameter(my_c_id);
            PAYMENT_7_GC->set_parameter(c_w_id);
            PAYMENT_7_GC->set_parameter(c_d_id);
            if(transaction->execute_statement(PAYMENT_7_GC) != ERROR_CODE::OK) {
                transaction->rollback();
                PG_RETURN_INT32(-1);
            }
	} else {
		char my_c_data[1000];
                DATESTAMP h_date;
                get_datestamp(h_date);

		sprintf(my_c_data, "%d %d %d %d %d %f %s ", my_c_id, c_d_id,
                        c_w_id, d_id, w_id, h_amount, h_date);
                strncat(static_cast<char *>(my_c_data),static_cast<char const *>(std::string(c_data).c_str()),500-strlen(static_cast<char *>(my_c_data)));
                PAYMENT_7_BC->set_parameter(h_amount);
                PAYMENT_7_BC->set_parameter(my_c_data);
                PAYMENT_7_BC->set_parameter(my_c_id);
                PAYMENT_7_BC->set_parameter(c_w_id);
                PAYMENT_7_BC->set_parameter(c_d_id);
                if(transaction->execute_statement(PAYMENT_7_BC) != ERROR_CODE::OK) {
                    transaction->rollback();
                    PG_RETURN_INT32(-1);
                }
	}

#if 0 // We don't care history table now.
        PAYMENT_8->set_parameter(my_c_id);
        PAYMENT_8->set_parameter(c_d_id);
        PAYMENT_8->set_parameter(c_w_id);
        PAYMENT_8->set_parameter(d_id);
        PAYMENT_8->set_parameter(w_id);
        PAYMENT_8->set_parameter(h_amount);
        PAYMENT_8->set_parameter(w_name);
        PAYMENT_8->set_parameter(d_name);
        PAYMENT_8->set_parameter(get_timestamp());
        if(transaction->execute_statement(PAYMENT_8) != ERROR_CODE::OK) {
            transaction->rollback();
            PG_RETURN_INT32(-1);
        }
#endif

        //	SPI_finish();
        transaction->commit();
	PG_RETURN_INT32(0);  // OK
}

} //  namespace tpcc
} //  namespace ogawayama
