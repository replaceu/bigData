package foreo.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import foreo.service.UserInfoService;
import foreo.template.ForeoKafkaTemplate;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.common.time.Time.seconds;

@Service
public class UserInfoServiceImpl implements UserInfoService {
    @Autowired
    ForeoKafkaTemplate foreoKafkaTemplate;
    @Override
    public void getUserInfoToTopic() {
        SingleOutputStreamOperator<JSONObject> kafkaSource = foreoKafkaTemplate.getKafkaSource("base-log", "base-log-app");
        //todo:按照mid进行分组
        KeyedStream<JSONObject, String> jsonDataStream = kafkaSource.keyBy(e -> {
            String json = e.getJSONObject("common").getString("mid");
            return json;
        });

        //todo:使用状态做新老用户校验
        SingleOutputStreamOperator<JSONObject> newFlagDataStream = jsonDataStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //todo:声明状态用于表示当前mid是否已经访问过
            private ValueState<String> firstVisitDateState;
            private SimpleDateFormat simpleDateState;

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //todo:取出新用户标记
                String isNew = value.getJSONObject("common").getString("is_new");
                if (isNew.equals("1")) {
                    //todo:取出状态数据并取出当前访问时间
                    String firstTime = firstVisitDateState.value();
                    Long timestamp = value.getLong("ts");
                    //todo:判断状态数据是否为null
                    if (firstTime != null) {
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //更新状态
                        firstVisitDateState.update(simpleDateState.format(timestamp));
                    }
                }
                return value;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
                simpleDateState = new SimpleDateFormat("yyyy-MM-dd");
            }
        });

        //todo:利用测输出流实现数据拆分，使用ProcessFunction将数据拆分成启动、曝光以及页面数据
        SingleOutputStreamOperator<String> resultDataStream = newFlagDataStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //提取start字段
                String start = jsonObject.getString("start");
                if (start != null && start.length() > 0) {
                    //todo:将启动日志输出到到侧输出流
                    context.output(new OutputTag<String>("start") {
                    }, jsonObject.toString());
                } else {
                    //todo:将页面数据输出到主流
                    collector.collect(jsonObject.toString());
                    //todo:不是启动数据继续判断是否是曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //todo:将曝光数据遍历写入侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            //取出单条曝光数据
                            JSONObject displaysJson = displays.getJSONObject(i);
                            //添加页面id，输出到侧输出流
                            displaysJson.put("pageId", jsonObject.getJSONObject("page").getString("pageId"));
                            context.output(new OutputTag<String>("display") {
                            }, displaysJson.toString());
                        }
                    }
                }
            }
        });
        //todo:将三个流的数据写入对应的kafka主题
        DataStream<String> startDatStream = resultDataStream.getSideOutput(new OutputTag<String>("start") {
        });
        DataStream<String> displayDatStream = resultDataStream.getSideOutput(new OutputTag<String>("display") {
        });
        resultDataStream.addSink(foreoKafkaTemplate.getKafkaSink("dwd_page_log"));
        resultDataStream.addSink(foreoKafkaTemplate.getKafkaSink("dwd_start_log"));
        resultDataStream.addSink(foreoKafkaTemplate.getKafkaSink("dwd_display_log"));

    }

    @Override
    public void getUserDailyFirstVisit() {
        //获取执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取kafka主题数据创建流
        String groupId ="unique-visit-app";
        String sourceTopic = "page-log";
        String sinkTopic = "unique-visit";

        FlinkKafkaConsumer<String> kafkaConsumer = foreoKafkaTemplate.getKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> dataStream = environment.addSource(kafkaConsumer);
        //todo:将每行数据转化成为Json对象
        SingleOutputStreamOperator<JSONObject> jsonDataStream = dataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String element, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject json = JSON.parseObject(element);
                    collector.collect(json);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, element);
                }
            }
        });

        //todo:按照mid分组,过滤掉不是今天第一次访问的数据
        KeyedStream<JSONObject, String> keyedDataStream = jsonDataStream.keyBy(e -> e.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filterDataStream = keyedDataStream.filter(new RichFilterFunction<JSONObject>() {
            //声明状态
            private ValueState<String> firstVisitState;
            private SimpleDateFormat dateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                ValueStateDescriptor<String> visitStateDescriptor = new ValueStateDescriptor<>("visitState", String.class);
                //todo:创建状态ttl配置项
                /**
                 * 在实际应用中，很多状态会随着时间的推移逐渐增长，如果不加以限制，最终就会导致存储空间的耗尽。一个优化的思路是直接在代码中调用.clear()方法去清除状态，
                 * 但是有时候我们的逻辑要求不能直接清除。这时就需要配置一个状态的“生存时间”（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除。
                 *
                 * 具体实现上，如果用一个进程不停地扫描所有状态看是否过期，显然会占用大量资源做无用功。状态的失效其实不需要立即删除，所以我们可以给状态附加一个属性，也就是状态的“失效时间”。
                 * 状态创建的时候，设置失效时间 = 当前时间 + TTL；之后如果有对状态的访问和修改，我们可以再对失效时间进行更新；当设置的清除条件被触发时（比如，状态被访问的时候，或者每隔一段时间扫描一次失效状态），就可以判断状态是否失效、从而进行清除了。
                 * .newBuilder()
                 * 状态 TTL 配置的构造器方法，必须调用，返回一个 Builder 之后再调用.build()方法就可以得到 StateTtlConfig 了。方法需要传入一个 Time 作为参数，这就是设定的状态生存时间。
                 * .setUpdateType()
                 * 设置更新类型。更新类型指定了什么时候更新状态失效时间，这里的 OnCreateAndWrite表示只有创建状态和更改状态（写操作）时更新失效时间。
                 * 另一种类型 OnReadAndWrite则表示无论读写操作都会更新失效时间，也就是只要对状态进行了访问，就表明它是活跃的，从而延长生存时间。这个配置默认为 OnCreateAndWrite。
                 * .setStateVisibility()
                 * 设置状态的可见性。所谓的“状态可见性”，是指因为清除操作并不是实时的，所以当状态过期之后还有可能基于存在，这时如果对它进行访问，能否正常读取到就是一个问题了。
                 * 这里设置的 NeverReturnExpired 是默认行为，表示从不返回过期值，也就是只要过期就认为它已经被清除了，应用不能继续读取；这在处理会话或者隐私数据时比较重要。对应的另一种配置是 ReturnExpireDefNotCleanedUp，就是如果过期状态还存在，就返回它的值。
                 * 除此之外，TTL 配置还可以设置在保存检查点（checkpoint）时触发清除操作，或者配置增量的清理（incremental cleanup），还可以针对 RocksDB 状态后端使用压缩过滤器（compaction filter）进行后台清理。
                 */
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                visitStateDescriptor.enableTimeToLive(ttlConfig);
                firstVisitState = getRuntimeContext().getState(visitStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //todo:取出上一次访问页面
                String lastPageId = value.getJSONObject("page").getString("last-page-id");
                if (lastPageId != null || lastPageId.length() <= 0) {
                    //todo:取出状态数据
                    String firstVisitDate = firstVisitState.value();
                    //todo:取出数据时间
                    Long timestamp = value.getLong("ts");
                    String currentDate = dateFormat.format(timestamp);
                    if (firstVisitDate == null || !firstVisitDate.equals(currentDate)) {
                        firstVisitState.update(currentDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }

            }
        });

    }

    /**
     * 跳出就是用户访问了网站的一个页面后就跳出，不再继续访问网站的其他页面，而跳出率就是用跳出次数除以访问次数
     * 需要抓住两个特征：该页面是用户近期访问的第一个页面，首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
     * 用户跳出事件本质上就是一个条件事件加一个超时事件的组合
     */

    @Override
    public void getUserJumpOutRate() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //todo:读取page_log主题数据创建流
        String sourceTopic = "page-log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "user-jump-detail";
        FlinkKafkaConsumer<String> kafkaConsumer = foreoKafkaTemplate.getKafkaConsumer(sourceTopic, groupId);
        DataStreamSource<String> sourceDataStream = environment.addSource(kafkaConsumer);
        //todo:将数据转化为Json对象
        WatermarkStrategy<JSONObject> getWatermarkStrategy =WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long timestamp) {
                return element.getLong("timestamp");
            }
        });
        SingleOutputStreamOperator<JSONObject> jsonDataStream = sourceDataStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonData = JSONObject.parseObject(value);
                    collector.collect(jsonData);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty"), value);
                }
            }
        }).assignTimestampsAndWatermarks(getWatermarkStrategy);

        KeyedStream<JSONObject, String> keyedStream = jsonDataStream.keyBy(e -> e.getJSONObject("common").getString("mid"));
        //todo:配置CEP表达式，定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last-page-id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).times(2).consecutive().within(org.apache.flink.streaming.api.windowing.time.Time.seconds(10));

        //根据表达式筛选流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //todo:提取事件和超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<>("timeOut");
        SingleOutputStreamOperator<JSONObject> selectDataStream = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        List<JSONObject> begin = map.get("begin");

                        return begin.get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        List<JSONObject> begin = map.get("begin");
                        return begin.get(0);
                    }
                });
        //侧输出流
        DataStream<JSONObject> userJumpDetailDataStream = selectDataStream.getSideOutput(timeOutTag);
        DataStream<JSONObject> resultDataStream = selectDataStream.union(userJumpDetailDataStream);
        resultDataStream.map(JSONAware::toJSONString).addSink(foreoKafkaTemplate.getKafkaSink(sinkTopic));

    }
    /**
     * 复合事件处理（Complex Event Processing，CEP）是一种基于动态环境中事件流的分析技术，事件在这里通常是有意义的状态变化，
     * 通过分析事件间的关系，利用过滤、关联、聚合等技术，根据事件间的时序关系和聚合关系制定检测规则，持续地从事件流中查询出符合
     * 要求的事件序列，最终分析得到更复杂的复合事件*/



}
