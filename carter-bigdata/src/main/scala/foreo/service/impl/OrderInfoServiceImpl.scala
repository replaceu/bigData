package foreo.service.impl

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import foreo.bean.{OrderDetailDo, OrderDo, OrderWideDo}
import foreo.service.OrderInfoService
import foreo.utils.{ForeoKafkaUtils, ForeoOffsetUtils, ForeoRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

class OrderInfoServiceImpl @Autowired()(redisTemplate:StringRedisTemplate)extends OrderInfoService {
  override def getOrderJoinDetailStream(): Unit = {
    //todo:读取spark配置文件
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("OrderWideApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderTopic = "order-info"
    val orderGroup ="order-group"
    val detailTopic = "order-detail"
    val detailGroup = "detail-group"

    //todo:获取两个数据流的各自的偏移量
    val orderOffsetMap: Map[TopicPartition, Long] = ForeoOffsetUtils.getOffsetFromRedis(orderTopic, orderGroup)
    val detailOffsetMap: Map[TopicPartition, Long] = ForeoOffsetUtils.getOffsetFromRedis(detailTopic, detailGroup)
    var orderRecordStream: InputDStream[ConsumerRecord[String, String]]=null
    var detailRecordStream: InputDStream[ConsumerRecord[String, String]]=null
    //todo:分别从偏移量处开始读取数据
    if(orderOffsetMap!=null&&orderOffsetMap.size>0){
      orderRecordStream= ForeoKafkaUtils.getKafkaStream(orderTopic, ssc, orderGroup, orderOffsetMap)
    }else{
      orderRecordStream = ForeoKafkaUtils.getKafkaStream(orderTopic,ssc,orderGroup)
    }
    if(detailOffsetMap!=null&&detailOffsetMap.size>0){
      detailRecordStream= ForeoKafkaUtils.getKafkaStream(orderTopic, ssc, orderGroup, orderOffsetMap)
    }else{
      detailRecordStream = ForeoKafkaUtils.getKafkaStream(orderTopic,ssc,orderGroup)
    }
    //todo:获取得到两个数据流新的偏移量
    var orderRanges: Array[OffsetRange] = null;
    val orderDStream: DStream[ConsumerRecord[String, String]] = orderRecordStream.transform(rdd => {
      orderRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    var detailRanges: Array[OffsetRange] = null;
    val detailDStream: DStream[ConsumerRecord[String, String]] = detailRecordStream.transform(rdd => {
      detailRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //todo:将两个数据流转化为对像形式的数据流
    val orderJsonStream: DStream[OrderDo] = orderDStream.map {
      e => {
        val orderStr: String = e.value()
        val orderDo: OrderDo = JSON.parseObject(orderStr, classOf[OrderDo])
        orderDo
      }
    }
    val detailJsonStream: DStream[OrderDetailDo] = detailDStream.map {
      e => {
        val detailStr: String = e.value()
        val detailDo: OrderDetailDo = JSON.parseObject(detailStr, classOf[OrderDetailDo])
        detailDo
      }
    }
    //todo:双流Join(orderJsonStream,detailJsonStream),同时使用滑动窗口计算
    val orderWindowStream: DStream[OrderDo] = orderJsonStream.window(Seconds(20), Seconds(5))
    val detailWindowStream: DStream[OrderDetailDo] = detailJsonStream.window(Seconds(20), Seconds(5))
    val orderKeyStream: DStream[(Long, OrderDo)] = orderWindowStream.map {
      e => {
        (e.id, e)
      }
    }
    val detailKeyStream: DStream[(Long, OrderDetailDo)] = detailWindowStream.map {
      e => {
        (e.orderId, e)
      }
    }
    val orderJoinDetailStream: DStream[(Long, (OrderDo, OrderDetailDo))] = orderKeyStream.join(detailKeyStream)
    //todo:在滑动窗口期间需要分区，同时针对orderId以及orderDetailId利用Redis去重
    val orderWideStream: DStream[OrderWideDo] = orderJoinDetailStream.mapPartitions {
      tupleItr => {
        val tupleList: List[(Long, (OrderDo, OrderDetailDo))] = tupleItr.toList
        val orderWideList: ListBuffer[OrderWideDo] = new ListBuffer[OrderWideDo]
        for ((orderId, (order, orderDetail)) <- tupleList) {
          val orderKey: String = "orderJoin" + orderId
          val script = "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end"
          val result: Long = redisTemplate.execute(new DefaultRedisScript[Long](script, classOf[Long]), util.Arrays.asList(orderKey), orderDetail.id)
          redisTemplate.opsForValue() set(orderKey, orderDetail.id, 30, TimeUnit.MINUTES)
          if (result == 1L) {
            orderWideList.append(new OrderWideDo(order, orderDetail))
          }
        }
        orderWideList.iterator
      }
    }
    //todo:对于join过得数据流实现订单金额的分摊
    val orderWideSplitStream: DStream[OrderWideDo] = orderWideStream.mapPartitions {
      orderTupleItr => {
        val orderWideList: List[OrderWideDo] = orderTupleItr.toList
        for (orderWide <- orderWideList) {
          //todo:从Redis中获取明细累加
          var orderOriginSumKey: String = "order-origin-sum" + orderWide.orderId
          var orderOriginSum: Double = 0D
          val sumValue: String = redisTemplate.opsForValue().get(orderOriginSumKey)
          if (sumValue != null && sumValue.size > 0) {
            orderOriginSum = sumValue.toDouble
          }
          var orderSplitSumKey: String = "order-split-sum" + orderWide.orderId
          var orderSplitSum: Double = 0L
          val splitValue: String = redisTemplate.opsForValue().get(orderSplitSumKey)
          if (splitValue != null && splitValue.size > 0) {
            orderSplitSum = splitValue.toDouble
          }

          //todo:判断是否是最后一条，从而实现分摊
          val detailAmount: Double = orderWide.orderPrice * orderWide.skuNum
          if (detailAmount == orderWide.originalTotalAmount - orderOriginSum) {
            orderWide.finalDetailAmount = Math.round((orderWide.finalTotalAmount - orderSplitSum) * 100d) / 100d
          } else {
            orderWide.finalDetailAmount = Math.round(orderWide.finalTotalAmount * detailAmount / orderWide.originalTotalAmount * 100) / 100d
          }

          //todo:更新Redis中的值
          var newOrderOriginSum = orderOriginSum + detailAmount
          redisTemplate.opsForValue.set(orderOriginSumKey, newOrderOriginSum.toString, 600, TimeUnit.SECONDS)

          var newOrderSplitSum = orderSplitSum + orderWide.finalDetailAmount
          redisTemplate.opsForValue().set(orderSplitSumKey, newOrderSplitSum.toString, 600, TimeUnit.SECONDS)
        }
        orderWideList.toIterator
      }
    }

    orderWideSplitStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          orderWide=>{
            ForeoKafkaUtils.sendToKafka("order-wide",JSON.toJSONString(orderWide,new SerializeConfig(true)))
          }
        }
        //todo:将新的偏移量重新存到Redis中
        ForeoOffsetUtils.saveOffsetToRedis(orderTopic,orderGroup,orderRanges)
        ForeoOffsetUtils.saveOffsetToRedis(detailTopic,detailGroup,detailRanges)
      }
    }
  }
}
