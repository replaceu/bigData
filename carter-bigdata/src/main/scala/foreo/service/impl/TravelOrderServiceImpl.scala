package foreo.service.impl

import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import foreo.mapper.HotAreaOrderMapper
import foreo.service.TravelOrderService
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.springframework.beans.factory.annotation.Autowired

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

class TravelOrderServiceImpl @Autowired()extends TravelOrderService{
  private val h3Core: H3Core = H3Core.newInstance()
  /**
   * 获取当日热区订单，当日新增热区订单
   */
  override def getHotAreaOrderToHbase(): Unit = {
    val sparkConf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[1]").setAppName("HotAreaOrder")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    sparkSession.udf.register("locationToH3Code",locationToH3Code _)
    sparkSession.udf.register("h3CodeToCoordinate" , h3CodeToCoordinate _)
    val date: Date = new Date()
    //todo:将经纬度转换成H3编码
    sparkSession.sql(HotAreaOrderMapper.orderHotTableDay(new SimpleDateFormat("yyyy-MM-dd").format(date))).createOrReplaceTempView("orderHotTableDay")
    //todo:得到当日的热区订单，生成dateFrame来处理每个街道的热区
    val todayHotAreaDataFrame: DataFrame = sparkSession.sql(HotAreaOrderMapper.getTodayHotArea).toDF("begin_address_code", "coordinate", "h3Code", "radius", "count")
    //注册临时表，用来做后面的当日新增热区
    todayHotAreaDataFrame.createOrReplaceTempView("todayHotAreaDataFrame")
    //todo:根据当日热区情况获取每个街道的热区
    val todayHotAreaRdd: RDD[Row] = todayHotAreaDataFrame.rdd
    //重新组装数据（街道编码,(街道编码,当日热区经纬度,当日热区半径,当日热区数量)）
    val thaData: RDD[(String, Iterable[(String, String, Int, Int)])] = todayHotAreaRdd.map {
      rdd => {
        //当日下单的街道经纬度编码
        val beginAddressCode = rdd.getAs[String]("begin_address_code")
        //当日热区经纬度坐标
        val coordinate = rdd.getAs[String]("coordinate")
        //当日热区半径
        val radius = rdd.getAs[Int]("radius")
        //当日热区数量
        val count = rdd.getAs[Int]("count")
        (beginAddressCode, coordinate, radius, count)
      }
    }.groupBy(e => e._1)
    //todo:返回当日对每个街道热区情况技术(街道编码，数量，详情)(begin_address_code,coordinate,radius,count)
    val addressRdd: RDD[(String, Int, List[(String, String, Int, Int)])] = thaData.map {
      line => {
        var count = 0
        for (data <- line._2) {
          count = count + data._4
        }
        (line._1, count, line._2.toList)
      }
    }
    //todo:求昨天的数据与今天的数据相比，新增的热区订单数量
    val calendar = new GregorianCalendar()
    calendar.setTime(date)
    calendar.add(date.getDate,-1)
    val yesterday: Date = calendar.getTime
    sparkSession.sql(HotAreaOrderMapper.orderHotTableDay(new SimpleDateFormat("yyyy-MM-dd").format(date))).createOrReplaceTempView("orderHotTableYesterday")
    sparkSession.sql(HotAreaOrderMapper.getYesterdayHotArea).createOrReplaceTempView("yesterdayHotAreaDataFrame")
    //根据昨日的热区订单，与今日热区订单做对比，得到日新增订单
    val yesterdayTodayHotAreaOrderDataFrame: DataFrame = sparkSession.sql(HotAreaOrderMapper.getNewHotAreaOrder).toDF("city_code", "coordinate", "radius", "count")
    val ythaoRdd: RDD[Row] = yesterdayTodayHotAreaOrderDataFrame.rdd
    //todo:依然对街道编码做分组，只不过这次是新增后的热区情况
    val ythaoData: RDD[(String, Iterable[(String, String, Int, Int)])] = ythaoRdd.map {
      rdd => {
        //当日下单的街道经纬度编码
        val cityCode = rdd.getAs[String]("city_code")
        //当日热区经纬度坐标
        val coordinate = rdd.getAs[String]("coordinate")
        //当日热区半径
        val radius = rdd.getAs[Int]("radius")
        //当日热区数量
        val count = rdd.getAs[Int]("count")
        (cityCode, coordinate, radius, count)
      }
    }.groupBy(e => e._1)

    val cityRdd: RDD[(String, Int, List[(String, String, Int, Int)])] = ythaoData.map {
      e => {
        var count = 0
        for (data <- e._2) {
          count = count + data._4
        }
        (e._1, count, e._2.toList)
      }
    }

    import sparkSession.sqlContext.implicits._
    val newOrderDataFrame: DataFrame = cityRdd.toDF("city_code", "city_num", "yesterday_hot_arr")
    val nowOrderDataFrame: DataFrame = addressRdd.toDF("city_code", "city_num", "today_hot_arr")
    //todo:创建新增和当天热区临时表
    newOrderDataFrame.createOrReplaceTempView("newOrderDataFrame")
    nowOrderDataFrame.createOrReplaceTempView("nowOrderDataFrame")
    //todo:当日热区订单和当日新增热区订单合并
    val hotOrder: DataFrame = sparkSession.sql(HotAreaOrderMapper.getHotOrder)
    hotOrder.show()

  }

  //经纬度转换成H3索引,resolution清晰度
  def locationToH3Code(lat:Double,lng:Double,resolution:Int):Long ={
    h3Core.geoToH3(lat,lng,resolution)
  }

  //根据h3编码，转成经纬度
  def h3CodeToCoordinate(h3Code:String):String={
    val geoCoord: GeoCoord = h3Core.h3ToGeo(h3Code.toLong)
    geoCoord.lat+","+geoCoord.lng
  }
}
