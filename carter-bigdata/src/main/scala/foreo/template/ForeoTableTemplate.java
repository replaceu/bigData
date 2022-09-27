package foreo.template;

public interface ForeoTableTemplate {
    //mysql对于配置数据初始化和维护管理，使用FlinkCDC读取配置信息表，将配置流作为广播流与主流进行连接
    void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend);
}
