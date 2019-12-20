# Guide for Prepared SQL in ogawayama 
2019.12.20 NT 堀川  

本メモでは、ogawayamaに試作したPrepared SQL機能の使用方法を記す。  
API_contracts.md等に記載された事項、例えば、
ogawayama-serverへの接続、トランザクション開始やクエリ結果を取り出す方法は記載しない。

## 概略の処理フロー
1. prepared SQLのためのデータを用意
2. prepare()を実行し、SQL文をparseする  
3. prepared_sql->set_parameter()を実行し、パラメータを設定する  
4. execute_\[statement|query\]()を実行する  

## 詳細
### Prepared SQL実行に必要なデータ
```
  PreparedStatementPtr prepared_sql;
```
* prepared_sqlは任意の名称。以下の説明ではデータ名として'prepared_sql'を使用する。

### SQL文のprepare
```
  if(connection->prepare(SQL文、prepared_sql) != ERROR_CODE::OK) {
      エラー処理
  }
```
* PrepareするSQL文で使用するパラメータ名はp1, p2, ...（固定）。  
* Prepare対象SQL文の一例："INSERT INTO tbl1 (clm1, clm2, clm3) VALUES(:p1, :p2, :p3)"  
* connectionについてはAPI_contracts.mdを参照のこと。

### パラメータ設定
```
  prepared_sql->set_parameter(1);        // パラメータ p1を設定
  prepared_sql->set_parameter(1.1);      // パラメータ p2を設定
  prepared_sql->set_parameter("ABCDE");  // パラメータ p3を設定
```
* 設定可能なパラメータの型は、std::int\[16|32|64\]_t, float, double, std::string_view（文字列全般）。
* パラメータにnullを設定する場合は、'set_parameter()'とする。  
* p1, p2, ...の順にパラメータを設定する。  
* set_parameter()の呼び出し回数は、SQL文で使用したパラメータ数と一致している必要がある。

### 実行
1) statementの場合
```
  ERROR_CODE error_code = transaction->execute_statement(prepared_sql.get()));
```

2) queryの場合
```
  ERROR_CODE error_code = transaction->execute_query(prepared_sql.get(), result_set));
```
* 以降の処理は、文字列としてSQL文を与えるexecute_statement()やexecute_transaction()と同じ。  
* transaction, result_setについてはAPI_contracts.mdを参照のこと。  

## その他
### header file
* Prepared SQLのために新たなheader fileをincludeする必要はない（#include "ogawayama/stub/api.h"のみ）。

### 現在の実装における制限事項
* 最初のset_parameter()からexecute_\[statement|query\]()の間に、（同一connectionに属する）別のprepared statementへの処理（set_parameter(), execute__\[statement|query\]()）を挟みこんではならない。
  * パラメータをstubからogawayama-serverに渡す経路は、１つのconnectionに属する全prepared statementで共通としているため。
* set_parameter()を行った場合、必ずexecute_\[statement|query\]()を実行する必要がある。
