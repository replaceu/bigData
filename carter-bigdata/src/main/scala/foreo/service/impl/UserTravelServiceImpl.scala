package foreo.service.impl

import com.alibaba.fastjson.JSONArray
import com.uber.h3core.H3Core
import foreo.bean.DistrictDo
import foreo.constants.ForeoConstants
import foreo.service.{DistrictsService, GeoPolygonService, UserTravelService}
import foreo.utils.ForeoHbaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.{GeometryFactory, Point, Polygon}
import org.locationtech.jts.io.WKTReader
import org.springframework.beans.factory.annotation.Autowired

import java.util
import scala.collection.mutable

/**
 * 1.从HBASE得到成都经纬度数据
 * 2.spark读取HBASE经纬度数据
 * 3.自定义UDF函数，计算每个经纬度的hashCode码值
 * 4.按照hashCode码值进行分组，获取每个六边形当中的经纬度个数
 * 5.每个六边形当中经纬度个数有多个，获取每个六边形中经纬度当中最小的哪一个代表这个虚拟车站
 * 6.每个区域都有很多虚拟车站，每个经纬度代表一个虚拟车站，统计每个区域虚拟车站的个数
 * 7.需要确定虚拟车站属于哪一个区域，同时区域边界的经纬度进行广播
 *
 */
class UserTravelServiceImpl @Autowired()(districtsService: DistrictsService,geoPolygonService: GeoPolygonService) extends UserTravelService{
  //todo:创建H3实例
  val h3: H3Core = H3Core.newInstance()

  def broadcastDistrictValue(sparkSession: SparkSession): Broadcast[java.util.ArrayList[DistrictDo]] = {
    //todo:获取每个区域的边界,每个区域画一个圈,判断经纬度是否在这个圈内
    val districtList: util.ArrayList[DistrictDo] = new util.ArrayList[DistrictDo]()
    val districts: JSONArray = districtsService.getCityDistricts("海口市", null)
    districtsService.parseDistrictInfo(districts, null, districtList)
    //todo:行政区域广播变量（spark开发优化的一个点）
    val districtBroadcast: Broadcast[java.util.ArrayList[DistrictDo]] = sparkSession.sparkContext.broadcast(districtList)
    districtBroadcast

  }

  override def getVirtualStation(): Unit = {
    val sparkConf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[1]").setAppName("SparkTravel")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")

    //todo:获取的到HBASE配置
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","ForeoNode01,ForeoNode02,ForeoNode03")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.setInt("hbase.client.operation.timeout",3000)
    val hbaseFrame: DataFrame = ForeoHbaseUtils.loadHBaseData(sparkSession, hbaseConf)
    //todo:将dataFrame注册成为一张表
    hbaseFrame.createOrReplaceTempView(ForeoConstants.OrderDataFrame)
    //todo:获取虚拟车站，每个虚拟车站里面所有的经纬度坐标点只取一个最小的
    val virtualRdd: RDD[Row] = getVirtualFrameRdd(sparkSession)
    //todo:广播每个区域的经纬度边界
    val districtBroadcast: Broadcast[java.util.ArrayList[DistrictDo]] = broadcastDistrictValue(sparkSession)
    //todo:将每个区域的边界转换成为一个多边形，使用Polygon这个对象来表示，返回一个元组（每一个区域封装对象District，多边形Polygon）
    val retRdd: RDD[mutable.Buffer[Row]] = virtualRdd.mapPartitions {
      eachPartition => {
        //使用JTS-Tools来通过多个经纬度，画出多边形
        import org.geotools.geometry.jts.JTSFactoryFinder
        val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
        val reader = new WKTReader(geometryFactory)
        //todo:将哪一个区的，哪一个边界求出来
        val districtPolygonTuple: mutable.Buffer[(DistrictDo, Polygon)] = geoPolygonService.changeDistrictToPolygon(districtBroadcast, reader)
        eachPartition.map(e => {
          val lng: String = e.getAs[String]("starting_lng")
          val lat: String = e.getAs[String]("starting_lat")
          val wktPoint: String = "POINT(" + lng + " " + lat + ")"
          val point: Point = reader.read(wktPoint).asInstanceOf[Point]
          val rows: mutable.Buffer[Row] = districtPolygonTuple.map(polygonTuple => {
            if (polygonTuple._2.contains(point)) {
              val fields: Array[Any] = e.toSeq.toArray ++ Seq(polygonTuple._1.getName)
              Row.fromSeq(fields)
            } else {
              null
            }
          }).filter(null != _)
          rows
        })

      }
    }
    //todo:针对retRdd的操作，写入HBASE
  }

  def getVirtualFrameRdd(sparkSession: SparkSession):RDD[Row] ={
    sparkSession.udf.register("locationToH3",new UDF3[String,String,Int,Long]() {
      override def call(lat: String, lng: String, result: Int): Long = {
        h3.geoToH3(lat.toDouble,lng.toDouble,result)
      }
    },DataTypes.LongType)
    val orderSql =
      s"""
         |select order_id,city_id,starting_lng,starting_lat,
         |locationToH3(starting_lng,starting_lat.12) as h3Code
         |from order_dataFrame
         |""".stripMargin
         //todo:在sql语句中使用h3接口进行六边形栅格化
    val gridDataFrame: DataFrame = sparkSession.sql(orderSql)
    gridDataFrame.createOrReplaceTempView("orderGrid")
    val virtualSql: String =
      s"""
         | select order_id,city_id,starting_lng,starting_lat,
         | row_number() over(partition by orderGrid.h3code order by starting_lng,starting_lat asc) rn
         | from order_grid  join
         | (select h3code,count(1) as totalResult from orderGrid  group by h3code having totalResult >=1) groupCount
         | on order_grid.h3code = groupCount.h3code
         | having(rn=1)
      """.stripMargin
    //todo:面的sql语句，将每个经纬度转换成为了一个HashCode码值，然后对hashCode码值分组，
    // 获取每个组里面经纬度最小的那一个，得到这个经纬度，然后再计算，这个经纬度坐落在哪一个区里面
    val virtualFrame: DataFrame = sparkSession.sql(virtualSql)
    //todo:判断每个虚拟车站一个经纬度，这个经纬度坐落在哪一个区里面，就能知道每个区里面有多少个虚拟车站了
    val virtualRdd: RDD[Row] = virtualFrame.rdd
    virtualRdd
  }
}
