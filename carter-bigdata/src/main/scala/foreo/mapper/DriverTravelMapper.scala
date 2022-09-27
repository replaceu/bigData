package foreo.mapper

object DriverTravelMapper {
  //当日有效订单
  lazy val selectEffectiveOrderToday=(day:String)=>
    s"""
       |select
       |city_name,count(1) as effective_num_day
       |from order
       |where
       |date_format(create_date,'yyyy-MM-dd') = '${day}' and cancel=0
       |group by city_name
       |""".stripMargin
  lazy val selectEffectiverOrderWeek=(week:String)=>
    s"""
       |select
       |city_name,count(1) as effective_num_week
       |from order
       |where
       |WEEKOFYEAR(create_time) = '${week}' and cancel = 0
       |group by city_name
       |""".stripMargin
  lazy val selectEffectiveOrderMonth=(month:String)=>
    s"""
       |select
       |city_name,count(1) as effective_num_day
       |from order
       |where
       |date_format(create_date,'yyyy-MM') = '${month}' and cancel = 0
       |group by city_name
       |""".stripMargin
  lazy val selectEffectiveOrderSeason=(season:String,year:String)=>
    s"""
       |select
       |city_name,count(1) as effective_num_season
       |from order
       |where
       |quarter(create_time) ='${season}' and
       |date_format(create_time,'yyyy') ='${year}'
       |and cancel = 0
       |group by city_name
       |""".stripMargin
  lazy val selectEffectiveOrderYear=(year:String)=>
    s"""
       |select
       |city_name,count(1) as effective_num_day
       |from order
       |where
       |date_format(create_date,'yyyy') = '${year}' and cancel = 0
       |group by city_name
       |""".stripMargin
  lazy val selectTotalOrderToday =(day:String)=>
    s"""
       |select
       |city_name,count(1) as total_num_today
       |from order
       |where date_format(create_time,'yyyy-MM-dd')='${day}'
       |group by city_name
       |""".stripMargin
  lazy val selectTotalOrderMonth =(month:String)=>
    s"""
       |select
       |city_name,count(1) as total_num_today
       |from order
       |where date_format(create_time,'yyyy-MM')='${month}'
       |group by city_name
       |""".stripMargin
  lazy val selectTotalOrderWeek =(week:String)=>
    s"""
       |select
       |city_name,count(1) as total_num_today
       |from order
       |where WEEKOFYEAR(create_time)='${week}'
       |group by city_name
       |""".stripMargin
  lazy val selectTotalOrderSeason =(season:String,year:String)=>
    s"""
       |select
       |city_name,count(1) as total_num_today
       |from order
       |where date_format(create_time,'yyyy')='${year}'
       |and quarter(create_time) ='${season}'
       |group by city_name
       |""".stripMargin
  lazy val selectTotalOrderYear =(year:String)=>
    s"""
       |select
       |city_name,count(1) as total_num_today
       |from order
       |where date_format(create_time,'yyyy')='${year}'
       |group by city_name
       |""".stripMargin
  lazy val selectPlatOrderToday=
    s"""
       |select city_name,
       |cast when effective_num_today=0 then '0.0%' else CONCAT(cast (round(effective_num_today*100/total_num_today,2)as string),'%') end as today_complete_rate
       |from
       |(select effectiveOrderToday.city_name,effectiveOrderToday.effective_num_today,totalOrderToday.total_num_today
       |from effectiveOrderToday left join totalOrderToday on effectiveOrderToday.city_name=totalOrderToday.city_name
       |)tb
       |""".stripMargin
  lazy val selectPlatOrderWeek=
    s"""
       |select city_name,
       |cast when effective_num_week=0 then '0.0%' else CONCAT(cast (round(effective_num_week*100/total_num_week,2)as string),'%') end as week_complete_rate
       |from
       |(select effectiveOrderWeek.city_name,effectiveOrderWeek.effective_num_week,totalOrderWeek.total_num_week
       |from effectiveOrderWeek left join totalOrderWeek on effectiveOrderWeek.city_name=totalOrderWeek.city_name
       |)tb
       |""".stripMargin
  lazy val selectPlatOrderMonth=
    s"""
       |select city_name,
       |cast when effective_num_month=0 then '0.0%' else CONCAT(cast (round(effective_num_month*100/total_num_month,2)as string),'%') end as month_complete_rate
       |from
       |(select effectiveOrderMonth.city_name,effectiveOrderMonth.effective_num_month,totalOrderMonth.total_num_month
       |from effectiveOrderMonth left join totalOrderMonth on effectiveOrderMonth.city_name=totalOrderMonth.city_name
       |)tb
       |""".stripMargin

  lazy val selectPlatOrderSeason=
    s"""
       |select city_name,
       |cast when effective_num_season=0 then '0.0%' else CONCAT(cast (round(effective_num_season*100/total_num_season,2)as string),'%') end as season_complete_rate
       |from
       |(select effectiveOrderSeason.city_name,effectiveOrderSeason.effective_num_season,totalOrderSeason.total_num_season
       |from effectiveOrderSeason left join totalOrderSeason on effectiveOrderSeason.city_name=totalOrderSeason.city_name
       |)tb
       |""".stripMargin
  lazy val selectPlatOrderYear=
    s"""
       |select city_name,
       |cast when effective_num_year=0 then '0.0%' else CONCAT(cast (round(effective_num_year*100/total_num_year,2)as string),'%') end as year_complete_rate
       |from
       |(select effectiveOrderYear.city_name,effectiveOrderYear.effective_num_year,totalOrderYear.total_num_year
       |from effectiveOrderYear left join totalOrderYear on effectiveOrderYear.city_name=totalOrderYear.city_name
       |)tb
       |""".stripMargin
  lazy val selectFinalSummaryPatOrder=
    s"""
       |select
       |cast(1 as string) as rk ,
       |cast(year_season_month_week_tb.city_name as  string) as city_name ,
       |cast(year_season_month_week_tb.year_complete_rate as string) as year_complete_rate,
       |cast(if(year_season_month_week_tb.season_complete_rate is null , '0.0%' , year_season_month_week_tb.season_complete_rate) as string) as season_complete_rate,
       |cast(if(year_season_month_week_tb.month_complete_rate is null , '0.0%' , year_season_month_week_tb.month_complete_rate) as string) as month_complete_rate,
       |cast(if(year_season_month_week_tb.week_complete_rate is null , '0.0%' , year_season_month_week_tb.week_complete_rate) as string) as week_complete_rate,
       |cast(if(platOrderToday.day_complete_rate is null , '0.0%' , platOrderToday.day_complete_rate) as string) as day_complete_rate
       |from
       |(select
       |year_season_month_tb.city_name ,
       |year_season_month_tb.year_complete_rate ,
       |year_season_month_tb.season_complete_rate ,
       |year_season_month_tb.month_complete_rate ,
       |platOrderWeek.week_complete_rate
       |from
       |(select
       |year_season_tb.city_name ,
       |year_season_tb.year_complete_rate ,
       |year_season_tb.season_complete_rate ,
       |platOrderMonth.month_complete_rate
       |from
       |(select
       |platOrderYear.city_name ,
       |platOrderYear.year_complete_rate ,
       |platOrderSeason.season_complete_rate
       |from
       |platOrderYear left join platOrderSeason
       |on
       |platOrderYear.city_name = platOrderSeason.city_name
       |) year_season_tb left join platOrderMonth
       |on
       |year_season_tb.city_name = platOrderMonth.city_name
       |) year_season_month_tb left join platOrderWeek
       |on
       |year_season_month_tb.city_name = platOrderWeek.city_name
       |) year_season_month_week_tb left join platOrderToday
       |on
       |year_season_month_week_tb.city_name = platOrderToday.city_name
       |""".stripMargin
}
