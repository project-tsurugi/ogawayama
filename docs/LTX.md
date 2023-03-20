# frontendからLong Transactionを使用する方法について
2023.03.20
NT horikawa

本メモは、backendがサポートするLong Transaction(LTX)機能をfrontendから使用する方法を記す。
具体的には、LTX開始を示すtransaction optionを付与してトランザクション開始要求を行うことで、LTXを使用できる。
なお、本メモはfrontendがそのユーザに提供するLTXインタフェースやfrontend内の実装方法については言及しない。

## Transaction option
Transaciton optionは下記の要素を持つ。

| 要素名 | 要素の型 | 取り得る値、意味など |
| :--- | :--- | :--- |
| type | TransactionType | SHORT, LONG, or READ_ONLY |
| priority | TransactionPriority | INTERRUPT, WATI, INTERRUPT_EXCLUDE, or WAIT_EXCLUDE |
| label | string | トランザクションを識別するための任意の文字列 <br> OCCの動作には影響しない |
| write_preserves | WritePreserve（string）の配列 | 書き込み先としてpreserveする表名の配列 |

注1）各要素の詳細については、tsubakuroのmodules/proto/src/main/protos/jogasaki/proto/sql/request.protoを参照のこと。
注2）transaction optionの要素は、必要に応じて追加される可能性がある。現時点で、read area（二種）の追加を予定している。

## API
LTXは、LTX開始を示すtransaction optionを付与してトランザクション開始要求を行うことで、LTXを使用できる。
そこで、LTXサポートのため、gawayamaがfrontendに提供する従来までのトランザクション開始要求（`ErrorCode begin(TransactionPtr&)`）に加え、
transaction optionを引数にもつトランザクション開始要求を加える。

```
    /**
     * @brief begin a transaction and get Transaction class.
     * @param ptree the transaction option
     * @param transaction returns a transaction class
     * @return error code defined in error_code.h
     */
    ErrorCode begin(const boost::property_tree::ptree&, TransactionPtr&);
```

### ptreeによるtransaction optionの指定

| 名前 | 型 | 説明 <br> （対応するtransaction option要素） | 備考 |
|----|----|----|----|
| "transactionType"     | TransactionType | type | TransactionTypeは、SHORT, LONG, READ_ONLYが定義されたenum |
| "transactionPriority" | TransactionPriority | priority | TransactionPriorityは、INTERRUPT, WAIT, INTERRUPT_EXCLUDE, WAIT_EXCLUDEが定義されたenum |
| "transactionLabel"    | std::string | label | - |
| "writePreserve"       | std::stringの配列 | write_preserves | 配列要素は、"tableName"を名前とするstd::string |

補足）名前の文字列やTransactionType, TransactionPriorityはogawayama/stub/transaction_option.hで定義されており、 `#include <ogawayama/stub/api.h>` により使用できる。
