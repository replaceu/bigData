package foreo.template.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import foreo.bean.TableProcessDo;
import foreo.constants.ForeoConstants;
import foreo.template.ForeoTableTemplate;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Component
public class ForeoTableTemplateImpl extends BroadcastProcessFunction<JSONObject, String, JSONObject> implements ForeoTableTemplate {

    private OutputTag<JSONObject> objectOutputTag;
    private MapStateDescriptor<String, TableProcessDo> mapStateDescriptor;
    private Connection connection;

    public ForeoTableTemplateImpl(OutputTag<JSONObject> objectOutputTag, MapStateDescriptor<String, TableProcessDo> mapStateDescriptor) {
        this.objectOutputTag = objectOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(ForeoConstants.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(ForeoConstants.PHOENIX_SERVER);
    }

    @Override
    public void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(ForeoConstants.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] fields = sinkColumns.split(",");

            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];

                //判断是否为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key ");
                } else {
                    createTableSQL.append(field).append(" varchar ");
                }
                //判断是否为最后一个字段,如果不是,则添加","
                if (i < fields.length - 1) {
                    createTableSQL.append(",");
                }
            }

            createTableSQL.append(")").append(sinkExtend);
            //打印建表语句
            System.out.println(createTableSQL);
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext context, Collector<JSONObject> out) throws Exception {
        //1.获取状态数据
        ReadOnlyBroadcastState<String, TableProcessDo> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        TableProcessDo TableProcessDo = broadcastState.get(key);

        if (TableProcessDo != null) {
            //2.过滤字段
            JSONObject data = value.getJSONObject("after");
            filterColumn(data, TableProcessDo.getSinkColumns());
            //3.分流
            //将输出表/主题信息写入Value
            value.put("sinkTable", TableProcessDo.getSinkTable());
            String sinkType = TableProcessDo.getSinkType();
            if (TableProcessDo.SINK_TYPE_KAFKA.equals(sinkType)) {
                //Kafka数据,写入主流
                out.collect(value);
            } else if (TableProcessDo.SINK_TYPE_HBASE.equals(sinkType)) {
                //HBase数据,写入侧输出流
                context.output(objectOutputTag, value);
            }
        } else {
            System.out.println("该组合Key：" + key + "不存在！");
        }
    }

    @Override
    public void processBroadcastElement(String value, Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcessDo TableProcessDo = JSON.parseObject(data, TableProcessDo.class);
        //2.建表
        if (TableProcessDo.SINK_TYPE_HBASE.equals(TableProcessDo.getSinkType())) {
            checkTable(TableProcessDo.getSinkTable(),
                    TableProcessDo.getSinkColumns(),
                    TableProcessDo.getSinkPk(),
                    TableProcessDo.getSinkExtend());
        }
        //3.写入状态,广播出去
        BroadcastState<String, TableProcessDo> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = TableProcessDo.getSourceTable() + "-" + TableProcessDo.getOperateType();
        broadcastState.put(key, TableProcessDo);
    }

    /**
     * @param data        {"id":"11","tm_name":"atguigu","logo_url":"aaa"}
     * @param sinkColumns id,tm_name
     *                    {"id":"11","tm_name":"atguigu"}
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

    }
}
