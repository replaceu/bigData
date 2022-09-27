package foreo.service.impl

import com.alibaba.fastjson.{JSON, JSONObject}
import foreo.service.MaxwellService
import foreo.utils.{ForeoKafkaUtils, ForeoOffsetUtils, ForeoPropUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.springframework.beans.factory.annotation.Autowired

import java.util.Properties


/**
 * 使用Maxwell+kafka使得数据同步
 */
class MaxwellServiceImpl @Autowired()extends MaxwellService{
  override def maxwellToDataBase(): Unit = {
    //todo:得到spark的配置
    val properties: Properties = ForeoPropUtils.load("config.properties")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dauApp").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "mall-db-maxwell"
    val groupId = "base-db-maxwell-group"

    //todo:从Redis当中获取偏移量
    var recordStream:InputDStream[ConsumerRecord[String,String]] =null;
    val offsetMap: Map[TopicPartition, Long] = ForeoOffsetUtils.getOffsetFromRedis(topic, groupId)
    if(offsetMap!=null&&offsetMap.size>0){
      //todo:kafka从偏移量的位置开始读取数据
      recordStream = ForeoKafkaUtils.getKafkaStream(topic, ssc, groupId,offsetMap)
    }else{
      recordStream = ForeoKafkaUtils.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange]=null
    //todo:获取当前采集周期中需要处理的数据对应的分区以及偏移量
    val offsetStream: DStream[ConsumerRecord[String, String]] = recordStream.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //todo:将offsetStream数据流封装成json对象
    val jsonStream: DStream[JSONObject] = offsetStream.map {
      record => {
        val jsonStr: String = record.value()
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        jsonObject
      }
    }
    //todo:将数据流分流到kafka不同的topic中
    jsonStream.foreachRDD{
      rdd=>{
        rdd.foreach{
          eachJson=>{
            val opType: AnyRef = eachJson.get("type")
            val dataJson: JSONObject = eachJson.getJSONObject("data")
            if (dataJson!=null&& !dataJson.isEmpty&& !opType.equals("delete")){
              //todo:获取需要更新的表名以及向kafka发送消息
              val tableName: String = dataJson.getString("table")
              val topic: String = "ods-" + tableName
              ForeoKafkaUtils.sendToKafka(topic,dataJson.toString())
            }
          }
        }
        //todo：将新的偏移量offset保存到Redis中
        ForeoOffsetUtils.saveOffsetToRedis(topic,groupId,offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
