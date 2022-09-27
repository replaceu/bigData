package foreo.service.impl

import foreo.mapper.DriverTravelMapper
import foreo.service.DriverTravelService
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.springframework.beans.factory.annotation.Autowired

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar}

class DriverTravelServiceImpl @Autowired()extends DriverTravelService {

  override def getDriverOrderToHbase(): Unit = {
    val sparkConf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[1]").setAppName("driverTravelApp")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("WARN")
    //当日平台有效订单总数
    val date = new Date()
    sparkSession.sql(DriverTravelMapper.selectEffectiveOrderToday(new SimpleDateFormat("yyyy-MM-dd").format(date))).createOrReplaceTempView("effectiveOrderToday")
    //本周平台有效订单总数
    val calendar = new GregorianCalendar()
    calendar.setTime(date)
    val week: Int = calendar.get(Calendar.WEEK_OF_YEAR)
    sparkSession.sql(DriverTravelMapper.selectEffectiverOrderWeek(week.toString)).createOrReplaceTempView("effectiveOrderWeek")
    //本月平台有效订单总数
    val month: Int = calendar.get(Calendar.MONTH)
    sparkSession.sql(DriverTravelMapper.selectEffectiveOrderMonth(month.toString)).createOrReplaceTempView("effectiveOrderMonth")
    //本季度平台有效订单数
    val season: Int = (month - 1) / 3
    val year: Int = calendar.get(Calendar.YEAR)
    sparkSession.sql(DriverTravelMapper.selectEffectiveOrderSeason(season.toString,year.toString)).createOrReplaceTempView("effectiveOrderSeason")
    //本年度平台有效订单数
    sparkSession.sql(DriverTravelMapper.selectEffectiveOrderYear(year.toString)).createOrReplaceTempView("effectiveOrderYear")
    //当日平台总计订单总数
    sparkSession.sql(DriverTravelMapper.selectTotalOrderToday(new SimpleDateFormat("yyyy-MM-dd").format(date))).createOrReplaceTempView("totalOrderToday")
    sparkSession.sql(DriverTravelMapper.selectTotalOrderWeek(week.toString)).createOrReplaceTempView("totalOrderWeek")
    sparkSession.sql(DriverTravelMapper.selectTotalOrderMonth(month.toString)).createOrReplaceTempView("totalOrderMonth")
    sparkSession.sql(DriverTravelMapper.selectTotalOrderSeason(season.toString,year.toString)).createOrReplaceTempView("totalOrderSeason")
    sparkSession.sql(DriverTravelMapper.selectTotalOrderYear(year.toString)).createOrReplaceTempView("totalOrderYear")

    //当天平台订单完成率
    sparkSession.sql(DriverTravelMapper.selectPlatOrderToday).createOrReplaceTempView("platOrderToday")
    sparkSession.sql(DriverTravelMapper.selectPlatOrderWeek).createOrReplaceTempView("platOrderWeek")
    sparkSession.sql(DriverTravelMapper.selectPlatOrderMonth).createOrReplaceTempView("platOrderMonth")
    sparkSession.sql(DriverTravelMapper.selectPlatOrderSeason).createOrReplaceTempView("platOrderSeason")
    sparkSession.sql(DriverTravelMapper.selectPlatOrderYear).createOrReplaceTempView("platOrderYear")

    //todo:日/周/月/季/年的平台订单完成率
    sparkSession.sql(DriverTravelMapper.selectFinalSummaryPatOrder).toDF("rk","city_name","year_complete_rate","week_complete_rate","month_complete_rate","season_complete_rate")
  }
}
