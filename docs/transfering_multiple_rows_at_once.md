# execute_query()に対して複数rowをserverからstubに一括転送する方式の検討
2019.12.10 NT 堀川

## 目的
現在のserverは、stubからEXECUTE_QUERYコマンドを受信した後、
NEXTコマンドを受信すると、１つのrowをrow_queueに書き込むようになっている。
すなわち、NEXTコマンド受信→実行エンジン経由でのrow取り出し→row_queueへのrow書き込み、
というシーケンスを1rowをserverからstubに転送するために行っている。

この一連の逐次動作がrow転送のスループットを下げている原因と考えられるため、
serverがNEXTコマンドを受信することなく（もしくは受信するNEXTコマンドの数を減らして）
複数のrowを一括転送する方式を検討する。

## 方式
以下の説明では、query結果としてstubに渡すrow数は多数存在するものとする。
End of row（EOR）時は、EORを示すデータをrow_queueに書き込んだ後、row転送操作を終える。

### dual buffer
バッファに格納可能出来るrowの数をNとした場合、
N/2個のrow毎にNEXTコマンドをstubからserverに送信する方式である。
これにより、NEXTコマンドに伴う逐次動作の回数を2/Nに減らすことができる。
なお、NEXTコマンドを送信する閾値はN/2である必然性はなく、2~N間の任意の値（例えば3N/4）で良い。

* server: EXECUTE_QUERY受信: N個のrowをrow_queueに書き込む
* stub: add-onからのNEXT要求: N/2回まではserverとインタラクションすることなくrow_queueのデータをadd-onに返す  
　　　　　　　　　　　　　　　　N/2回目にNEXTコマンド（row数指定あり）をserverに送信
* server: NEXT受信: N/2個のrowをrow_queueに書き込む。この際、書き込むrow数はrow_queueのメンバ変数で指定する。

### 自発push
serverはrow_queueに書き込み可能なrow数を書き込んだ後、row_queueを監視する。
そして（stubがrow_queueからrowを取り出したことで）、
row_queueに空きが生じたことを検知すると、row_queueに新たなrowを書き込む。

* 利点：stub→serverへのNEXTコマンド受け渡しをなくせる
* 欠点：以下のケースでは複数の待ち要因を監視する必要が生じるため、制御が複雑となる。
  * add-onからのrow読み出しがEORに達する前に終わった場合（row_queueとchannel_streamの同時に監視が必要）
  * 複数のrow_queueが同時に使用される場合（複数row_queueの同時監視が必要）


## V1で採用する方式
制御を単純化できる利点より、dual buffer方式とする。
Nは、NEXT削減効果を発揮するため、10以上（現時点の実装では32）とする。
