# Frontend-backend連携における型の扱いと型情報の表現方法について
2019.07.29 NT 堀川

このドキュメントは、NEDOプロ最終段階での実施項目Ⅱ-①とⅡ-②連携機能（ogawayama）における型の扱いと
型情報の表現方法について規定するとともに、V0での実装範囲を示す。

## 基本方針
### 扱うデータ型
本連携機能が扱うデータ型は、frontendとして利用するPostgreSQLが扱うデータ型（補足参照）における
基本データ型およびその単純な組み合わせで実現できる範囲（複合型）の中から、
必要性*)を勘案して選択する。  
ここで
「基本データ型」とはメモリに配置したときの長さが決まっているもの（固定データ長）または可変長文字列を指す。
「単純な組み合わせ」とは複数個の予め決められた基本データ型を決められた順番で並べたデータ型（struct）または
単一の基本データ型を可変長個並べたデータ型(array)、もしくは、
その組み合わせ（structの最後の要素がarrayとなっているデータ型）を指す。  
*) 必要性は、一般的な業務システムで必要不可欠やNEDOプロ実施項目Ⅲの実証実験で使用、という観点で判断する。

### 型の表現（型情報）
型の表現には、連携の両側（PostgreSQLとShakujo）から独立した第三の型情報を使用する。

## 型情報の概要
型情報にはデータ長等の属性が必要となるため、単純な数値（enum）ではなく、クラスによりデータ型を表現する。

### 基本型、V0での実装範囲
ogawayamaでは、以下を基本型とする。

| 大分類 | PostgreSQLの型（名） | ogawayamaの型（名） |
|-:|:-|:-|
|整数|smallint|INT16|
|整数|integer|INT32|
|整数|bigint|INT64|
|浮動小数点|real|FLOAT32
|浮動小数点|double precision|FLOAT64
|文字列|character<br>character varying<br>text| TEXT  
|十進数|numeric<br>decimal<br>（PostgreSQLでは両者は同じ）|-（v0では扱わない）
|時間間隔|interval|-（v0では扱わない）
|貨幣金額|money|-（v0では扱わない）
|日付|date|-（v0では扱わない）
|時刻|time<br>time with time zone|-（v0では扱わない）
|日付と時刻|timestamp<br>timestamp with time zone|-（v0では扱わない）

### 複合型
現時点では実装方法の詳細は未定。


## V1以降における実装範囲
現時点では、V1以降に追加する基本型の範囲および複合型の実装方法と範囲は未定。
別途、frontend担当（NEC）と協議して決める。


## 補足
### PostgreSQLがサポートするデータ型
https://www.postgresql.jp/document/11/html/datatype.html に列挙されているデータ型は下記。

| 名称 | 別名 | 説明 | 分類 |
|-:|:-|:-|:-:|
| bigint | int8 | 8バイト符号付き整数 | b
bigserial | serial8 | 自動増分8バイト整数 | b(s)
bit [ (n) ] |   | 固定長ビット列 | b
bit varying [ (n) ] | 可変長ビット列[ (n) ] | | b
boolean | bool | 論理値（真/偽） | b
box |   | 平面上の矩形 | c
bytea |   | バイナリデータ <br> （「バイトの配列（byte array）」） | b
character [ (n) ] | char [ (n) ] | 固定長文字列 | b
character varying [ (n) ] | varchar [ (n) ] | 可変長文字列 | b
cidr |   | IPv4もしくはIPv6 <br> ネットワークアドレス | b
circle |   | 平面上の円 | c
date |   | 暦の日付（年月日） | b
double precision | float8 | 倍精度浮動小数点（8バイト） | b
inet |   | IPv4もしくはIPv6ホストアドレス | b
integer | int, int4 | 4バイト符号付き整数 | b
interval [ fields ] [ (p) ] |   | 時間間隔 | b
json |   | テキストのJSONデータ | n
jsonb |   | バイナリ JSON データ, 展開型 | n
line |   | 平面上の無限直線 | c
lseg |   | 平面上の線分 | c
macaddr |   | MAC（Media Access Control） <br> アドレス | b
macaddr8 |   | MAC (Media Access Control)  <br> アドレス (EUI-64 形式) | b
money |   | 貨幣金額 | b
numeric [ (p, s) ] | decimal [ (p, s) ] | 精度の選択可能な高精度数値 | b
path |   | 平面上の幾何学的経路 | c(v)
pg_lsn |   | PostgreSQLログシーケンス番号 | p
point |   | 平面上の幾何学的点 | c
polygon |   | 平面上の閉じた幾何学的経路 | c(v)
real | float4 | 単精度浮動小数点（4バイト） | b
smallint | int2 | 2バイト符号付き整数 | b
smallserial | serial2 | 自動増分2バイト整数 | b(s)
serial | serial4 | 自動増分4バイト整数 | b(s)
text |   | 可変長文字列 | b
time [ (p) ] <br> [ without time zone ] |   | 時刻（時間帯なし） | b
time [ (p) ] <br> with time zone | timetz | 時間帯付き時刻 | b
timestamp [ (p) ] <br> [ without time zone ] |   | 日付と時刻（時間帯なし） | b
timestamp [ (p) ] <br> with time zone | timestamptz | 時間帯付き日付と時刻 | b
tsquery |   | テキスト検索問い合わせ | n
tsvector |   | テキスト検索文書 | n
txid_snapshot |   | ユーザレベルの <br> トランザクションIDスナップショット | p
uuid |   | 汎用一意識別子 | b or c <br>（128ビット数値のため）
xml |   | XMLデータ | n

「分類」に示したマークの意味は下記。
* b: 基本データ型としての扱いが可能
* b(s)：基本データ型（整数）だが、自動増分の扱いを検討する必要あり
* c：複数個の基本データ型の単純な組み合わせ
* c(v)：複数個の基本データ型の単純な組み合わせだが、データ個数が可変となる可能性あり
* p：PostgreSQL固有のデータ
* n：複数個の基本データ型の単純な組み合わせでは表現できない
pとnはumikongoではサポートしない（予定）。