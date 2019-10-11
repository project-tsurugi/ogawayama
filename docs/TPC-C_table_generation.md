# TPC-Cベンチマーク用の表作成とデータのセーブ・ロード
2019.09.19 NT 堀川

TPC-Cベンチマークの表データを生成するツール
（ogawayama/build下のexamples/tpcc_loader/ogawayama-tpcc-loader）を用意した。
本メモでは、その使用方法を記す。

## ogawayama-tpcc-loaderのコマンド行オプション
| オプション名 | 型 | 意味 | デフォルト値 | 
|-:|:-|:-|:-|
| generate | bool | 表データを作成する | false |
| warehouse | int | TPC-Cのスケールファクタ | 0 |

## TPC-Cデータの作成とセーブ
ogawayama-serverを立ち上げた状態で、以下を順に実行する。
* ogawayama-tpcc-loader -generate [-warehouse W]   
　表データを作成する。Wはスケールファクタ。  
　スケールファクタが指定されない場合（または値が０の場合）は、ミニチュア（tiny）サイズのデータを生成する。
* ogawayama-cli -dump  
  表データをファイルにセーブする。
  保存場所は、ogawayama-serverのlocationオプション（defaultは"./db"）で指定した名前のディレクトリ。
* ogawayama-cli -terminate  
  ogawayama-serverを終了する。

## TPC-Cデータのロード
ogawayama-serverを立ち上げた状態で、以下を順に実行する。
* ogawayama-tpcc-loader
  オプションが指定されない場合は、create tableのみを実行する  
  この操作が必要となるのは、V0の表データ・セーブ・ロード機能では表定義（スキーマ）情報は扱われないため、
  create tableを行ってから表データをロードする必要があることによる。
* ogawayama-cli -load
  ファイルに保存された表データを読み込む。  
  読み込むファイルの場所は、ogawayama-serverのlocationオプション（defaultは"./db"）で指定した名前のディレクトリ。
