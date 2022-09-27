package foreo.service.impl;

import com.alibaba.fastjson.JSON;
import foreo.dto.OrderDetailInfoDto;
import foreo.dto.OrderInfoDto;
import foreo.dto.OrderWideInfoDto;
import foreo.service.OrderWideService;
import foreo.template.ForeoKafkaTemplate;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.util.Collector;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.SimpleDateFormat;

public class OrderWideServiceImpl implements OrderWideService {
    @Autowired
    ForeoKafkaTemplate foreoKafkaTemplate;
    @Override
    public void getOrderWideStream() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //todo:读取kafka订单和订单明晰主题数据
        String orderInfoSourceTopic = "order_info";
        String orderDetailSourceTopic = "order_detail";
        String orderWideSinkTopic = "order_wide";
        String groupId = "order_wide_group";
        FlinkKafkaConsumer<String> orderConsumer = foreoKafkaTemplate.getKafkaConsumer(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderDataStream = environment.addSource(orderConsumer);

        FlinkKafkaConsumer<String> detailConsumer = foreoKafkaTemplate.getKafkaConsumer(orderDetailSourceTopic, groupId);
        DataStreamSource<String> detailDataStream = environment.addSource(detailConsumer);

        //todo:订单和订单明细关联（双流join）
        WatermarkStrategy<OrderInfoDto> orderWatermarkStrategy =WatermarkStrategy.<OrderInfoDto>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderInfoDto>() {
            @Override
            public long extractTimestamp(OrderInfoDto orderInfoDto, long l) {
                return orderInfoDto.getCreateTs();
            }
        });
        KeyedStream<OrderInfoDto, Long> orderInfoKeyedStream = orderDataStream.map(e -> {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //todo:将json字符串转换成为JavaBean
            OrderInfoDto orderInfo = JSON.parseObject(e, OrderInfoDto.class);
            String createDate = orderInfo.getCreateDate();
            String[] createTimeArr = createDate.split(" ");
            orderInfo.setCreateDate(createTimeArr[0]);
            orderInfo.setCreateHour(createTimeArr[1]);
            orderInfo.setCreateTs(dateFormat.parse(createDate).getTime());
            return orderInfo;
        }).assignTimestampsAndWatermarks(orderWatermarkStrategy).keyBy(OrderInfoDto::getId);

        WatermarkStrategy<OrderDetailInfoDto> detailWatermarkStrategy = WatermarkStrategy.<OrderDetailInfoDto>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderDetailInfoDto>() {
            @Override
            public long extractTimestamp(OrderDetailInfoDto orderDetailInfoDto, long l) {
                return orderDetailInfoDto.getCreateTs();
            }
        });
        KeyedStream<OrderDetailInfoDto, Long> detailInfoDtoKeyedStream = detailDataStream.map(e -> {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            OrderDetailInfoDto detailInfo = JSON.parseObject(e, OrderDetailInfoDto.class);
            detailInfo.setCreateTs(dateFormat.parse(detailInfo.getCreateTime()).getTime());
            return detailInfo;
        }).assignTimestampsAndWatermarks(detailWatermarkStrategy).keyBy(OrderDetailInfoDto::getOrderId);
        //todo:使用intervalJoin实现双流join
        SingleOutputStreamOperator<OrderWideInfoDto> orderWideDataStream = orderInfoKeyedStream.intervalJoin(detailInfoDtoKeyedStream).between(Time.seconds(-5), Time.seconds(5)).process(new ProcessJoinFunction<OrderInfoDto, OrderDetailInfoDto, OrderWideInfoDto>() {
            @Override
            public void processElement(OrderInfoDto orderInfoDto, OrderDetailInfoDto orderDetailInfoDto, ProcessJoinFunction<OrderInfoDto, OrderDetailInfoDto, OrderWideInfoDto>.Context context, Collector<OrderWideInfoDto> collector) throws Exception {
                collector.collect(new OrderWideInfoDto(orderInfoDto, orderDetailInfoDto));
            }
        });
    }
}
