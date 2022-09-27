package foreo.constants;

public interface ForeoConstants {
    String HTAB_HAIKOU_ORDER ="HTAB_HAIKOU_ORDER";
    String DEFAULT_FAMILY = "DEFAULT_FAMILY";
    String HBASE_OFFSET_STORE_TABLE = "HBASE_OFFSET_STORE_TABLE";
    String HBASE_OFFSET_FAMILY_NAME= "HBASE_OFFSET_FAMILY_NAME";
    String OrderDataFrame = "OrderDataFrame";

    //Phoenix库名
    String HBASE_SCHEMA = "GMALL_REALTIME";

    //Phoenix驱动
    String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

    //ClickHouse_Url
    String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
}
