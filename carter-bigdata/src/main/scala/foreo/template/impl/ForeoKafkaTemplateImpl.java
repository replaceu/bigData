package foreo.template.impl;

import com.alibaba.fastjson.JSONObject;
import foreo.template.ForeoKafkaTemplate;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class ForeoKafkaTemplateImpl implements ForeoKafkaTemplate {
    private static String kafkaServer = "hadoop101:9092,hadoop102:9092,hadoop103:9092";

    @Override
    public FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }

    @Override
    public SingleOutputStreamOperator<JSONObject> getKafkaSource(String topic, String groupId) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        FlinkKafkaConsumer<String> kafkaConsumer = getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaSource = environment.addSource(kafkaConsumer);
        SingleOutputStreamOperator<JSONObject> retStream = kafkaSource.map(JSONObject::parseObject);
        return retStream;
    }

    @Override
    public SinkFunction<String> getKafkaSink(String topic) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.severs",kafkaServer);
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }


}
