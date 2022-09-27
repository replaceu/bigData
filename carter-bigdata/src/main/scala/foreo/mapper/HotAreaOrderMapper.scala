package foreo.mapper

object HotAreaOrderMapper {
  //todo:通过udf函数计算当日热区订单数-精度7，大概半径为1200米
  lazy val orderHotTableDay = (time:String)=>
    s"""
       |select open_lat,open_lng,create_time,begin_address_code
       |locationToH3Code(open_lat,open_lng,7) as h3Code
       |from order
       |where date_format(create_time,'yyyy-MM-dd')='${time}'
       |""".stripMargin
  lazy val getTodayHotArea=
    """
      |select table2.begin_address_code,h3CodeToCoordinate(table2.h3Code) coordinate,
      |table2.h3Code,1200 radius,table2.rank as count
      |from
      |(select *,row_number() over(partition by h3Code order by rank desc) num
      |from
      |(select *,row_number() over(partition by h3Code order by create_time desc) rank
      |from orderHotTableDay
      |)table
      |)table2 where table2.num=1
      |""".stripMargin
  lazy val getYesterdayHotArea =
    s"""
       |select table2.begin_address_code,h3CodeToCoordinate(table2.h3Code) coordinate,
       |table2.h3Code,1200 radius,table2.rank as count
       |from
       |(select *,row_number() over(partition by h3Code order by rank desc) num
       |from
       |(select *,row_number() over(partition by h3Code order by create_time desc) rank
       |from orderHotTableYesterday
       |)table
       |)table2 where table2.num=1
       |""".stripMargin
  lazy val getNewHotAreaOrder =
    s"""
       |select
       |if(today_begin_address_code is null,yesterday_begin_address_code,today_begin_address_code) as city_code,
       |if(today_coordinate is null,yesterday_coordinate,today_coordinate) as coordinate,
       |if(today_radius is null,yesterday_radius,today_radius) as radius,
       |if(today_count is null,0-if(yesterday_count is null , 0 , yesterday_count) , today_count-if(yesterday_count is null,0,yesterday_count)) as count
       |from
       |(
       |select
       |getTodayHotArea.begin_address_code as today_begin_address_code,
       |getTodayHotArea.coordinate as today_coordinate,
       |getTodayHotArea.radius as today_radius,
       |getTodayHotArea.count as today_count,
       |getYesterdayHotArea.begin_address_code as yesterday_begin_address_code,
       |getYesterdayHotArea.coordinate as yesterday_coordinate,
       |getYesterdayHotArea.radius as yesterday_radius,
       |getYesterdayHotArea.count as yesterday_count
       |from getTodayHotArea
       |full outer join getYesterdayHotArea
       |on getTodayHotArea.h3Code = getYesterdayHotArea.h3Code
       |)tb
       |""".stripMargin
  lazy val getHotOrder =
    s"""
       |select
       |cast(nowOrderDataFrame.city_code as String) as rk,
       |cast(nowOrderDataFrame.city_code as String) as city_code,
       |cast(nowOrderDataFrame.city_num as String) as now_order_num,
       |cast(newOrderDataFrame.city_num as String) as new_order_num
       |from nowOrderDataFrame
       |left join newOrderDataFrame
       |on nowOrderDataFrame.city_code=newOrderDataFrame.city_code
       |""".stripMargin

}
