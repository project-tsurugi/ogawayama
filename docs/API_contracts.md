# ogawayama contracts API Guide 
2019.09.24 NT 堀川  
2019.09.25 NT 堀川　Revise (reviewコメント反映）  
2019.09.26 NT 堀川　R2 (状態の定義を見直し）  

本メモでは、ogawayama APIのcontractsを記す。

## Contract Status
フロントエンドとバックエンド（DBサーバ）とのインタフェースは、
Stub, Connection, Transactionオブジェクトを介して行う。
これら３種類のオブジェクトは各々１個作成して1:1:1対応させる使い方が通常であるが、
API仕様としては、一つのStubオブジェクトに複数のConnectionオブジェクト・ペアを
対応させることも可能となっている（connectionとtransactionオブジェクトは1:1対応）。
このため、フロントエンドにおけるバックエンドとのインタフェース状態は、
Stub状態とConnection状態の２種類で規定する。

### Stub状態
* Ready：接続先serverの存在を確認し、接続する準備が整った状態

### Connection状態
* Undefined：Connectionオブジェクトが取得されていない状態。
* Connected：serverとの接続が完了した状態かつトランザクションが実行されていない状態
* TxStarted：serverがトランザクションを実行している状態

### ResultSet状態
DBサーバに処理要求したクエリの結果は、ResultSetオブジェクト（Connection状態がTxStartedの場合にのみ有効）
によりフロントエンドに戻される。
その状態（ResultSet状態）は、ResultSetオブジェクトの状態とオブジェクト内の読み込み対象column番号により規定する。
* ResultSetオブジェクト状態
  * Invalid：無効な状態（この状態のResultSetにアクセスしたときの動作は不定）
  * Opened：DBサーバによる通信路への接続が完了し、かつ、DBサーバがResultSetオブジェクトにmetadataを設定した状態
  * Valid：DBサーバがResultSetオブジェクトに読み込み対象tupleを設定した状態
  * EndOfRow：DBサーバによる読み込み対象tupleの設定が完了した状態（DBサーバが読み込み対象tupleとしてEndOfRowを設定した状態）
* 読み込み対象column番号（ResultSet状態がValidの場合のみ有効）：フロントエンドが次に読み込むcolumnの番号  
なお、読み込み対象column番号はResultSetオブジェクト内部でのみ使用する変数であり、フロントエンドが陽に使用することはない。

## ErrorCode
stubがAPI（次節）経由で受け付けたフロントエンドからの処理要求（Request）に対して返すErrorCodeは下記。
* OK：正常終了
* COLUMN_WAS_NULL：読み込み対象columnはNULLであることを示す
* END_OF_ROW：読み込み対象tupleの最後まで読みが完了していることを示す
* END_OF_COLUMN：ResultSetオブジェクト内の読み込み対象column番号が読み込み対象tupleのcolumn数を超えたことを示す
* COLUMN_TYPE_MISMATCH：vの型が読み込み対象columnの型と一致していないことを示す  
* UNSUPPORTED：execute_statement()やexecute_transaction()で渡されたSQL文はバックエンドがサポートしていないものであることを示す
* UNKNOWN：execute_statement()やexecute_transaction()で渡されたSQL文の処理においてUNSUPPORTED以外のエラーが発生したことを示す

## Request(API)
本節では、stubが受け付けるRequestを示す。

### stub = make_stub(std::string_view name = "ogawayama")
* 処理内容：nameで識別されるDBサーバと接続する環境（stub）を準備する
* 条件
  * 事前条件：なし
  * 事後条件：Stub状態 == Ready

### error_code = stub->get_connection(番号, connection)
* 処理内容：DBサーバと接続する
* 条件
  * 事前条件：Connection状態 == Undefined
　　　　　番号 == フロントエンド・ワーカー・プロセスを一意に識別する1～N（Nは最大ワーカー・プロセス数）の整数値
  * 事後条件：error_codeは処理結果を示す値、error_codeがOKの場合はConnection状態はConnectedとなる  
  　　cf. Nを事前にDBサーバに通知することはしない。DBサーバは必要に応じてNを動的に拡張する。  
  　　　　なお、ある番号で取得されたconnectionに対しての複数プロセスやスレッドによる同時並行アクセスは不可とする（行った場合の動作は不定）。
  * 不変条件：Stub状態 == Ready

### error_code = connection->begin(transaction)
* 処理内容：connectionで接続されたDBサーバのワーカー・スレッドがトランザクション処理を開始する
* 条件
  * 事前条件：Connection状態 == Connected
  * 事後条件：error_codeは処理結果を示す値、error_codeがOKの場合はConnection状態はTxStartedとなる
  * 不変条件：Stub状態（任意）

### error_code = transaction->execute_statement(std::string_view sql_statement)
* 処理内容：transactionで示されるトランザクション処理としてsql_statementを実行する
* 条件
  * 事前条件：なし
  * 事後条件：DBサーバにてsql_statementを実行、error_codeには処理結果を示す値が入る
  * 不変条件：Stub状態（任意）、Connection状態 == TxStarted

### error_code = transaction->execute_query(std::string_view sql_query, result_set)
* 処理内容：transactionで示されるトランザクション処理としてsql_queryを実行する
* 条件
  * 事前条件：ResultSetオブジェクト状態は任意  
  　　　　　なお、与えられたResultSetオブジェクト状態が（前に実行されたexecute_query()により）Opened等となっている場合、  
  　　　　　TransactionオブジェクトはResultSetオブジェクト状態をInvalidに遷移させたうえでexecute_query()の実行を開始する。
  * 事後条件：error_codeには処理結果を示す値が入る、error_codeがOKの場合はResultSetオブジェクト状態はOpenedとなる。
  * 不変条件：Stub状態（任意）、Connection状態 == TxStarted

### error_code = result_set->get_metadata(metadata)
* 処理内容：result_setのメタデータ（tuple情報）を取り出す
* 条件
  * 事前条件：なし
  * 事後条件：error_codeは処理結果を示す値、error_codeがOKの場合はmetadataに有効な値が入る。
  * 不変条件：Stub状態（任意）、Connection状態 == TxStarted、  
  　　　　　ResultSetオブジェクト状態 == Opened, Valid, or EndOfRow（変化しない）

### error_code = result_set->next()
* 処理内容：result_setに読み込み対象tupleを設定する
* 条件
  * 事前条件：ResultSetオブジェクト状態 == Opened または Valid（この場合、読み込み対象column番号は任意）
  * 事後条件：error_codeは処理結果を示す値が入る
    * error_codeがOKの場合：ResultSetオブジェクト状態はValid、読み込み対象column番号は1となる
    * error_codeがEndOfRowの場合：ResultSetオブジェクト状態はEndOfRow
  * 不変条件：Stub状態（任意）、Connection状態 == TxStarted  
  　　cf. ResultSetオブジェクト状態がEndOfRowのときにnext()を実行したときの動作は不定。  

### error_code = result_set->next_column(v)
* 処理内容：result_setの読み込み対象tupleから、読み込み対象column番号（n）で示されるcolumnの値（v）を取得する
* 条件
  * 事前条件：読み込み対象column番号はn  
  　　　　　vは読み込み対象columnのデータ型（metadataから判明）に対応するデータ型の変数
  * 事後条件：error_codeは処理結果を示す値
    * error_codeがOKの場合：読み込み対象column番号（n）で示されるcolumnの値がvに設定され、読み込み対象column番号はn+1となる  
    * error_codeがCOLUMN_WAS_NULLの場合：vは不定、読み込み対象column番号はn+1となる
    * error_codeがEND_OF_COLUMNの場合：vは不定、読み込み対象column番号nは変化しない
    * error_codeがCOLUMN_TYPE_MISMATCHの場合：vは不定、読み込み対象column番号はn+1となる
  * 不変条件：Stub状態（任意）、Connection状態 == TxStarted、  
　　　　　ResultSetオブジェクト状態 == Valid

### error_code = transaction->commit()
* 処理内容：transactionで示されるトランザクション処理をcommitする
* 条件
  * 事前条件：Connection状態 == TxStarted  
  * 事後条件：Connection状態 == Connected、  
  　　　　　トランザクション処理中にopenしたResultSetオブジェクトの状態は総てInvalidとなる。

### error_code = transaction->rollback()
* 処理内容：transactionで示されるトランザクション処理をabortする
* 条件（transaction->commit()と同じ）