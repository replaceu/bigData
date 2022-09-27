package foreo.service.impl

import com.alibaba.fastjson.{JSON, JSONObject}
import foreo.bean.DauInfoDo
import foreo.service.UserDayService
import foreo.utils.{ForeoElasticSearchUtils, ForeoKafkaUtils, ForeoOffsetUtils, ForeoPropUtils, ForeoRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jvnet.hk2.annotations.Service
import org.springframework.beans.factory.annotation.Autowired
import redis.clients.jedis.Jedis


import java.lang
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable.ListBuffer

/**
 * 用户日或统计类
 */
@Service
class UserDayServiceImpl @Autowired()extends UserDayService{

  override def getUserDailyLoginInfo(): Unit = {
    //todo:得到spark的配置
    val properties: Properties = ForeoPropUtils.load("config.properties")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dauApp").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //todo:使用kafka消费数据
    val groupId = "mall-dau-bak"
    val topic = "mall-start-bak"
    //todo:从redis中读取kafka偏移量
    val kafkaOffsetMap: Map[TopicPartition, Long] = ForeoOffsetUtils.getOffsetFromRedis(topic, groupId)
    var recordStream: InputDStream[ConsumerRecord[String, String]]=null
    if(kafkaOffsetMap!=null&&kafkaOffsetMap.size>0){
      //todo:拿到偏移量后需要kafka从偏移量处读取数据流
       recordStream = ForeoKafkaUtils.getKafkaStream(topic, ssc, groupId, kafkaOffsetMap)
    }else{
      //todo:redis当中没有保存偏移量，就需要重新从最新处得到偏移量
      recordStream = ForeoKafkaUtils.getKafkaStream(topic, ssc, groupId)
    }

    //todo:获取当前采集周期从Kafka中消费的数据的起始偏移量以及结束偏移量值
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordStream.transform {
      rdd => {
        //因为recodeStream底层封装的是KafkaRDD，混入了HasOffsetRanges特质，这个特质中提供了可以获取偏移量范围的方法
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonString: String = record.value()
        //将json格式字符串转换为json对象
        val jsonObject: JSONObject = JSON.parseObject(jsonString)
        //从json对象中获取时间戳
        val ts: lang.Long = jsonObject.getLong("ts")
        //将时间戳转换为日期和小时  2020-10-21 16
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateStrArr: Array[String] = dateStr.split(" ")
        var dt = dateStrArr(0)
        var hr = dateStrArr(1)
        jsonObject.put("dt", dt)
        jsonObject.put("hr", hr)
        jsonObject
      }
    }
    //todo:通过Redis对采集到的启动日志进行去重操作,以分区为单位对数据进行处理，每一个分区获取一次Redis的连接
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        //以分区为单位对数据进行处理,每一个分区获取一次Redis的连接
        val jedis: Jedis = ForeoRedisUtils.getJedisClient()
        //定义一个集合，用于存放当前分区中第一次登陆的日志
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
        //对分区的数据进行遍历
        for (jsonObj <- jsonObjItr) {
          //获取日期
          val dt = jsonObj.getString("dt")
          //获取设备id
          val mid = jsonObj.getJSONObject("common").getString("mid")
          //拼接操作redis的key
          var dauKey = "dau:" + dt
          val isFirst = jedis.sadd(dauKey, mid)
          //设置key的失效时间
          if (jedis.ttl(dauKey) < 0) {
            jedis.expire(dauKey, 3600 * 24)
          }
          if (isFirst == 1L) {
            //说明是第一次登录
            filteredList.append(jsonObj)
          }
        }
        jedis.close()
        filteredList.toIterator
      }
    }

    //todo:将数据批量的保存到ES中,保存完成之后需要将消费偏移量放入Redis中
    filteredDStream.foreachRDD{
      rdd=>{
        //以分区为单位对数据进行处理
        rdd.foreachPartition{
          jsonObjItr=>{
            val dauInfoList: List[(String,DauInfoDo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfoDo(
                  commonJsonObj.getString("mid"), commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"), commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"), jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )
                (dauInfo.mid,dauInfo)
              }
            }.toList
            //将数据批量的保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            ForeoElasticSearchUtils.bulkInsert(dauInfoList,"mall-dau-info" + dt)
          }
        }
        //todo:提交偏移量到Redis中
        ForeoOffsetUtils.saveOffsetToRedis(topic,groupId,offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }



}
