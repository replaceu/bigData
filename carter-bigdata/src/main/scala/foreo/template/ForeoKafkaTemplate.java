package foreo.template;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public interface ForeoKafkaTemplate {
    FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId);

    SingleOutputStreamOperator<JSONObject> getKafkaSource(String topic,String groupId);

    SinkFunction<String> getKafkaSink(String topic);

}
