# Frontendによるtupleの処理（sketch）
2019.08.05 NT 堀川　First edition
2019.08.06 NT 堀川　Revise 1

本メモは、stub経由で実行エンジンから戻されるselect結果（tuple）を受け取る処理の概要を示す。

## 概略の処理フロー
1. execute_query()を実行し、result_setを取得  
2. result_set->get_metadata()を実行し、metadataを取得  
3. result_set->next()を実行  
4. metadataにある型情報に従ってresult_setからcolumnの値を取得  
5. result_setが複数行の場合は、3.と4.を繰り返す

## select結果を受け取る処理の概要
### execute_query()
```
  if(transaction->execute_query(SQL文、result_set) != stub::ErrorCode::OK) {
      エラー処理
  }
```

### get_metadata()
```
  if(result_set->get_metadata(metadata) != stub::ErrorCode::OK) {
      エラー処理
  }
```

### next()
```
  if((stub::ErrorCode error_code = result_set->next()) != stub::ErrorCode::OK) {
      if(error_code == END_OF_ROW) {
          処理対象行が存在しない状態が期待通りであれば、処理を正常終了する
      }
      エラー処理
  }
```

### result_setからcolumnの値を取得
```
  int i = 1;
  for(const auto& column_type : metadata->get_types()) {
    switch(column_type->get_type()) {
      case Metadata::Type::INT32:
      {
        std::int32_t v;
        if((stub::ErrorCode error_code = result_set->next_column<std::int32_t>(v)) == stub::ErrorCode::OK) {
          vをDatumに格納
        } else if(error_code == stub::ErrorCode::COLUMN_WAS_NULL) {
          当該columnをnullとする（ようにDatumを設定）
        } else {
          エラー処理
        }
        break;
      }
      case Metadata::Type::FLOAT64:
      {
        double v;
        if((stub::ErrorCode error_code = result_set->next_column<double>(v)) == stub::ErrorCode::OK) {
          vをDatumに格納
        } else if(error_code == stub::ErrorCode::COLUMN_WAS_NULL) {
          当該columnをnullとする（ようにDatumを設定）
        } else {
          エラー処理
        }
        break;
      }
      case Metadata::Type::TEXT:
      {
        std::string_view v;
        if((stub::ErrorCode error_code = result_set->next_column<std::string_view>(v)) == stub::ErrorCode::OK) {
          vをDatumに格納
        } else if(error_code == stub::ErrorCode::COLUMN_WAS_NULL) {
          当該columnをnullとする（ようにDatumを設定）
        } else {
          エラー処理
        }
        break;
      }
    }
  }

```  
上例は、V0で実装するtypeの内、INT32, FLOAT64, TEXTのみの処理を記載した。その他の型（INT16, INT64, FLOAT32）については、同様の処理で値を受け取るものとする。

### result_setが複数行の場合
下記のloopにより、result_set->next()がstub::ErrorCode::OKの間、処理対象行からcolumnを取り出す処理を繰り返す。
```
  while((error_code = result_set->next()) == stub::ErrorCode::OK) {
      result_setからcolumnの値を取得;
  }  
  if(error_code != END_OF_ROW) {
    エラー処理
  }
```

## APIにおける設計上の選択肢
### INT16,32,64やFLOAT32,64の扱い
本案におけるINTとFLOATの扱いは、stubにデータ長に応じたgetterを用意しておき、add-onではデータ長に応じたgetterを使うようにしている。
これに対し、例えば、mongo_fdw（https://github.com/citusdata/mongo_fdw）では、データ長を意識せず、INTとFLOATのgetterだけを用意し、getterの内部でデータ長の違いを吸収する処理を行っている。  
本案は、stubから渡すデータがPostgreSQLが想定してるデータ長に収まっているかどうかの判定を、型情報だけから実施できる（容易）という点を重視した。