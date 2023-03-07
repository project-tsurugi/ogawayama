#pragma once

/* TransactionOption consists of the following fields
    + TransactionType      enum TransactionType
    + TransactionPriority  enum TransactionPriority
    + TransactionLabel     std::string
    + WritePreserve        array of std::string
      + TableName            std::string (the name of a table to be write preserved)
      + ...                  the same as the above
 *
 * They are supposed to be stored in a boost::property_tree::ptree and passed to Connection::begin() API.
 * Use the labels on the left above for field names in the ptree
 */

namespace ogawayama::stub {
    /**
     * @brief Field name constant indicating the transaction type.
     */
    static constexpr const char* TRANSACTION_TYPE = "TransactionType";
    /**
     * @brief Field name constant indicating the transaction priority.
     */
    static constexpr const char* TRANSACTION_PRIORITY = "TransactionPriority";
    /**
     * @brief Field name constant indicating the transaction label.
     */
    static constexpr const char* TRANSACTION_LABEL = "TransactionLabel";
    /**
     * @brief Field name constant indicating the write preserve.
     */
    static constexpr const char* WRITE_PRESERVE = "WritePreserve";
    /**
     * @brief Field name constant indicating the table name in write preserve.
     */
    static constexpr const char* TABLE_NAME = "TableName";

    enum TransactionType {
        TRANSACTION_TYPE_UNSPECIFIED = 0,
        SHORT = 1,
        LONG = 2,
        READ_ONLY = 3,
    };

    enum TransactionPriority {
        TRANSACTION_PRIORITY_UNSPECIFIED = 0,
        INTERRUPT = 1,
        WAIT = 2,
        INTERRUPT_EXCLUDE = 3,
        WAIT_EXCLUDE = 4,
    };

}  // ogawayama::stub
