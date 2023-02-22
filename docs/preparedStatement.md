
# prepared statement API
2023.02.22
NT horikawa

backendがサポートするprepared statement機能をfrontendから使う際のAPIを規定する。
規定するのは、1) SQL文をコンパイルしてprepared statementにするprepare処理と
prepared statementにパラメータを付与して実行するexecute処理の２種類。

## API
`using namespace ogawayama::stub;` を前提に記述する。

### prepare
class Connectionに下記APIを設置。
* ErrorCode prepare(std::string_view, const std::vector<std::pair<std::string_view, Metadata::ColumnType>&, PreparedStatementPtr&);
  * std::string_view : SQL文
  * std::vector<std::pair<std::string_view, Metadata::ColumnType>> : placeholderの配列

### execute
class Transactionに下記APIを設置。
* ErrorCode execute_statement(PreparedStatement*, const std::vector<std::pair<std::string_view, VALUE>>&);
* ErrorCode execute_query(PreparedStatement*, const std::vector<std::pair<std::string_view, VALUE>>&, ResultSetPtr&);

VALUEは下記
std::variant<std::monostate, std::int32_t, std::int64_t, float, double, std::string, date_type, time_type, timestamp_type>
nullをVALUEとして指定する場合はstd::monostateを設定する。

cf prepareにより取得するPreparedStatementPtrはstd::unique_ptr<PreparedStatement>。
このため、std::unique_ptrのowner shipをexecuteに移動させないため、executeにはPreparedStatement*を渡すようになっている。


## 補足（protocol buffersによるrequest）
### prepare
#### request
* the prepare request.
message Prepare {
  string sql = 1;
  repeated Placeholder placeholders = 2;
}

#### parameters
当面のサポートは、atom typeのみ
* placeholder for the prepared statements.
message Placeholder {
    // the placeholder location.
    oneof placement {
        // the placeholder name.
        string name = 2;
    }
    reserved 3 to 10;

    // the placeholder type.
    oneof type_info {
        // the atom type.
        common.AtomType atom_type = 11;
        // the row type.
        common.RowType row_type = 12;
        // the user defined type.
        common.UserType user_type = 13;
    }
    reserved 14 to 19;

    // the type dimension for array types.
    uint32 dimension = 20;
}

第一版では、
INT4, INT8, FLOAT4, FLOAT8, CHARACTER, DATE, TIME_OF_DAY, TIME_POINT
をサポートする。
* atom type
enum AtomType {
    // unspecified type.
    TYPE_UNSPECIFIED = 0;

    // boolean type.
    BOOLEAN = 1;

    reserved 2, 3;

    // 32-bit signed integer.
    INT4 = 4;

    // 64-bit signed integer.
    INT8 = 5;

    // 32-bit floating point number.
    FLOAT4 = 6;

    // 64-bit floating point number.
    FLOAT8 = 7;

    // multi precision decimal number.
    DECIMAL = 8;

    // character sequence.
    CHARACTER = 9;

    reserved 10;

    // octet sequence.
    OCTET = 11;

    reserved 12;

    // bit sequence.
    BIT = 13;

    reserved 14;

    // date.
    DATE = 15;

    // time of day.
    TIME_OF_DAY = 16;

    // time point.
    TIME_POINT = 17;

    // date-time interval.
    DATETIME_INTERVAL = 18;

    // time of day with time zone.
    TIME_OF_DAY_WITH_TIME_ZONE = 19;

    // time point with time zone.
    TIME_POINT_WITH_TIME_ZONE = 20;

    // character large objects.
    CLOB = 21;

    // binary large objects.
    BLOB = 22;

    reserved 23 to 30;

    // unknown type.
    UNKNOWN = 31;

    reserved 32 to 99;
}

### execute
#### request
* the execute prepared statement request.
message ExecutePreparedStatement {
  common.Transaction transaction_handle = 1;
  common.PreparedStatement prepared_statement_handle = 2;
  repeated Parameter parameters = 3;
}

* the execute prepared query request.
message ExecutePreparedQuery {
  common.Transaction transaction_handle = 1;
  common.PreparedStatement prepared_statement_handle = 2;
  repeated Parameter parameters = 3;
}

#### parameters
* the parameter set for the statement.
message ParameterSet {
    // a list of parameters.
    repeated Parameter elements = 1;
}

第一版では、
int4_value, int8_value, float4_value, float8_value, character_value, date_value, time_of_day_value, time_point_value
をサポートする。
* the placeholder replacements.
message Parameter {
    // the placeholder location.
    oneof placement {
        // the placeholder name.
        string name = 2;
    }
    reserved 3 to 10;

    // the replacement values (unset describes NULL).
    oneof value {
        // boolean type.
        bool boolean_value = 11;

        // 32-bit signed integer.
        sint32 int4_value = 14;

        // 64-bit signed integer.
        sint64 int8_value = 15;

        // 32-bit floating point number.
        float float4_value = 16;

        // 64-bit floating point number.
        double float8_value = 17;

        // multi precision decimal number.
        common.Decimal decimal_value = 18;

        // character sequence.
        string character_value = 19;

        // octet sequence.
        bytes octet_value = 21;

        // bit sequence.
        common.Bit bit_value = 23;

        // date (number of days offset of epoch 1970-01-01).
        sint64 date_value = 25;

        // time of day (nano-seconds since 00:00:00).
        uint64 time_of_day_value = 26;

        // time point.
        common.TimePoint time_point_value = 27;

        // date-time interval.
        common.DateTimeInterval datetime_interval_value = 28;

        // time of day with time zone.
        common.TimeOfDayWithTimeZone time_of_day_with_time_zone_value = 29;

        // time point with time zone.
        common.TimePointWithTimeZone time_point_with_time_zone_value = 30;

        // reference column position (for load action).
        uint64 reference_column_position = 51;

        // reference column name (for load action).
        string reference_column_name = 52;
    }

    reserved 12, 13, 20, 22, 24, 31 to 40, 41 to 50, 53 to 99;
}
