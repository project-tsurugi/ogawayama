# Guide for Prepared SQL in ogawayama 
2019.12.20 NT 堀川  
2023.02.22 NT 堀川  

本メモでは、ogawayamaに試作したPrepared SQL機能の使用方法を記す。  
API_contracts.md等に記載された事項、例えば、
ogawayama-serverへの接続、トランザクション開始やクエリ結果を取り出す方法は記載しない。

## 概略の処理フロー
1. prepared SQLのためのデータを用意
2. prepare()を実行し、SQL文をparseする  
3. parameters_typeの変数にパラメータを設定する  
4. execute_\[statement|query\]()を実行する  

## 詳細
### Prepared SQL実行に必要なデータ
```
  PreparedStatementPtr prepared_sql;
```
* prepared_sqlは任意の名称。以下の説明では変数名として'prepared_sql'を使用する。

### SQL文のprepare
```
  if(connection->prepare(SQL文、placeholders, prepared_sql) != ERROR_CODE::OK) {
      エラー処理
  }
```
* connectionについてはAPI_contracts.mdを参照のこと。

### パラメータ設定
パラメータはparameters_type(std::vector<std::pair<std::string_view, VALUE>)に設定する。
以下の説明では変数名として'parameters'を使用する。
```
  parameters.emplace(name1, 1);        // パラメータ name1を設定
  parameters.emplace(name2, 1.1);      // パラメータ name2を設定
  parameters.emplace(name3, "ABCDE");  // パラメータ name3を設定
```
* 設定可能なパラメータの型は、std::int\[32|64\]_t, float, double, std::string_view（文字列全般）、date_type, time_type, timestamp_type。
* パラメータにnullを設定する場合は、値に'std::monostate'を設定する。  

### 実行
1) statementの場合
```
  ERROR_CODE error_code = transaction->execute_statement(prepared_sql.get(), parameters));
```

2) queryの場合
```
  ERROR_CODE error_code = transaction->execute_query(prepared_sql.get(), parameters, result_set));
```
* 以降の処理は、文字列としてSQL文を与えるexecute_statement()やexecute_transaction()と同じ。  
* transaction, result_setについてはAPI_contracts.mdを参照のこと。  

## その他
### header file
* Prepared SQLのために新たなheader fileをincludeする必要はない（#include "ogawayama/stub/api.h"のみ）。
