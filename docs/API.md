# Stub API案
2019.07.11 NT 堀川

このドキュメントは、NEDOプロ実施項目Ⅱ-①とⅡ-②連携部分（add-on + stub）の中核となるstub（開発コード名：Ogawayama stub）のAPIの設計方針と概略の仕様（基本的なオブジェクト、典型的な処理フローに関わるメソッドの機能と名前）を記述する。APIの詳細（パラメータと戻り値の意味とデータ型、getter等）は別途規定する。

## 基本方針
データベース操作API仕様は、コネクション、トランザクション、結果セットを基本概念とし、
各基本概念について論理に整合するメソッドを持つオブジェクトを配置することを基本とする。

## Stub API概要（V0にて実装）
### オブジェクト
StubAPI(V0)では、以下のオブジェクトを使用する。
| オブジェクト名 | 説明 | 取得方法 | 
|-:|:-|:-|
| Stub | バックエンドとのAPIを提供するオブジェクト | 最初から用意されているstaticなオブジェクト |
| Connection | データベースとの接続(セッション)に <br> 対応するオブジェクト | Stubオブジェクトのget_connection()により取得 |
| Transaction | トランザクションを実行するためのオブジェクト | Connectionオブジェクトのbegin()により取得 |
| ResultSet | Selectの結果セットを表すデータの表 | Transactionオブジェクトのexecute_query()により取得 |
| ResultSetMetaData | ResultSetの列の型を表すデータ | ResultSetオブジェクトのget_meta_data()により取得 |

### 各オブジェクトが備えるメソッド
#### Connection
| 関数名 | 説明 |
|-:|:-|
| begin() | トランザクションを開始をstubに伝達する <br> Callerは、そのトランザクションに対応するオブジェクトを戻り値として取得する |

#### Transaction
| 関数名 | 説明 |
|-:|:-|
| execute_query() | データベースに対する問合せ（クエリ）を行う |
| execute_statement() | データベースに対するデータ操作を行う |
| commit() | トランザクションを終了（コミット）する | 
| rollback() | トランザクションを中止する |

#### ResultSet
| 関数名 | 説明 |
|-:|:-|
| get_meta_data() | ResultSetの列の数と型（Select結果型）を示す <br> ResultSetMetaDataオブジェクトを取得する |
| next() | 結果セットから、次の行を取得する | 

### 典型的な処理フロー
本API経由でadd-onからトランザクションを実行する際の典型的な処理フローは以下の通り。
1. connection = Stub->get_connection();　　　　　// DBに接続（コネクション・オブジェクト取得）
2. transaction = connection->begin();　　　　　　// トランザクション開始（トランザクション・オブジェクト取得）
3. rs = transaction->execute_query(SQL文); 　　　 // クエリ実行要求
4. while( rs->next() != ) { rsを使った処理 }　　　　 // クエリ結果を使う処理
5. transaction->execute_statement(SQL文);　　　　// データ処理要求
6. transaction->commit();　　　　　　　　　　　　// トランザクション終了  
結果セット（rs）の開放とコネクション（connection）の切断（と関連リソースの開放）はdestructorにより行う。


### 要検討事項
#### stubからadd-onへの情報の戻し方
処理結果とエラー情報の２種類を受け渡す必要がある。各々、下記を基本とする。
* 処理結果：関数に値を受け取るための引数を与え、その引数を経由して処理結果を受け渡す。
* エラー情報：各関数の戻り値として受け渡す。現時点での実装案は下記の２種類（両者のPros. Cons.から判断して一方を選択する）。
  * 戻り値をエラーオブジェクトへのポインタを包含するunique_ptrとし、エラーがない場合は、nullptrを包含するunique_ptrを戻す。
  * 戻り値はboolとし、エラーが起きた場合はエラー・オブジェクトを処理要求先オブジェクト（Connection, Transaction等）に設定する。  


#### Autocommitモード
Autocommitを実現する方法として可能性のある考えられる実装方法を以下に示す。
* begin()のオプショナルな引数にautocommitかどうかを示すフラグを持たせる。
* begin()によりTransactionオブジェクトを取得、execute_query()やexecute_statement()のオプショナルな引数にautocommitかどうかを示すフラグを持たせる。
* Autocommitのexecute_query()やexecute_statement()は、Connectionオブジェクトに要求する。  
なお、Autocommitのサポートは、必要に応じてV1以降で検討することになる予定。

#### ResutSetからのcolumn値取り出し
現時点では、JDBCと同様、現在行（currentの指す行）から列値を取得するgetterメソッド(get_boolean、get_longなど)を提供することを想定している。
但し、V1以降の実装において、より適切と考えられる取り出し方法の具体案が見つかれば、そちらもサポート（若しくは変更する）可能性はある。

#### セッションのネスト
現案では、接続（Connection）とセッションが1:1に対応している。このため、V1以降でセッション途中でのuser切り替え（PostgreSQLの\connectコマンド）をサポートするためには、接続とセッションを分離する必要がある。  
V1以降においてユーザ切り替えをサポートする場合は、意義、必要性をか否か

### （補足）JDBC仕様との違い
#### SQLの実行要求
本案では、Conecctionオブジェクトのbegin()により取得したTransactionオブジェクトのexecute_query()やexecute_statement()によりSQLを実行させている。一方、JDBCでは、ConecctionオブジェクトのcreateStatement()によりStatementオブジェクトを取得し、SQL文の実行要求を行うようになっている。また、commit(), rollback()もConecctionオブジェクトに持たせている。
本案では、トランザクションを基本概念の一つに位置付けているので、トランザクションに関係する処理はTransactionオブジェクトが受け付けるフローとするのが論理的に整合していると考えられる。

#### Autocommitの扱い
JDBCでは、ConnectionオブジェクトのsetAutoCommit()により、autocommitのon, offを制御することで、begin()を不要としている。本案では、トランザクション開始を伝えるbegin()を使うことにしているが、そこでは、必ずしも実行エンジンにトランザクション開始を伝える必要はなく、stubで折り返す実装も可能である（begin()のオーバヘッドを実質的にゼロにできる）との判断から、begin()を持たせる欠点は皆無と考え、setAutoCommit()は使わない方針とした。