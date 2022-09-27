package foreo.utils


import foreo.bean.HaiKouOrderDo
import foreo.constants.ForeoConstants

import java.util.Date
import java.util.regex.Pattern
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.hbase.util.{Base64, Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ForeoHbaseUtils extends Logging with Serializable {
  def getHbaseConfig():Configuration={
    val hbaseConf: Configuration = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.setInt("hbase.client.operation.timeout", 3000)
    hbaseConf
  }

  // 对指定的列构造rowKey,采用Hash前缀拼接业务主键的方法
  def rowKeyWithHashPrefix(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val hash_prefix = getHashCode(rkString)
    val rowKey = Bytes.add(Bytes.toBytes(hash_prefix), Bytes.toBytes(rkString))
    rowKey
  }

  // 对指定的列构造rowKey, 采用Md5 前缀拼接业务主键方法，主要目的是建表时采用MD5 前缀进行预分区
  def rowKeyWithMD5Prefix(separator:String,length: Int,column: String*): Array[Byte] = {
    val columns = column.mkString(separator)
    var md5Prefix = MD5Hash.getMD5AsHex(Bytes.toBytes(columns))
    if (length < 8){
      md5Prefix = md5Prefix.substring(0, 8)
    }else if (length >= 8 || length <= 32){
      md5Prefix = md5Prefix.substring(0, length)
    }
    val row = Array(md5Prefix,columns)
    val rowKey = Bytes.toBytes(row.mkString(separator))
    rowKey
  }

  // 直接拼接业务主键构造rowKey
  def rowKey(column:String*):Array[Byte] = Bytes.toBytes(column.mkString(""))

  // Hash 前缀的方法：指定列拼接之后与最大的Short值做 & 运算
  // 目的是预分区，尽量保证数据均匀分布
  private def getHashCode(field: String): Short ={
    (field.hashCode() & 0x7FFF).toShort
  }

  def getStreamingContextFromHBase(streamingContext: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], group: String,matchPattern:String): InputDStream[ConsumerRecord[String, String]] = {
    val connection: Connection = getHbaseConnection
    val admin: Admin = connection.getAdmin
    var getOffset:collection.Map[TopicPartition, Long]  = getOffsetFromHBase(connection,admin,topics,group)
    val result = if(getOffset.size > 0){
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams,getOffset)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      value
    }else{
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      value
    }
    admin.close()
    connection.close()
    result
  }


  def getOffsetFromHBase(connection: Connection,admin: Admin,topics: Array[String], group: String): collection.Map[TopicPartition, Long] = {
    if(!admin.tableExists(TableName.valueOf(ForeoConstants.HBASE_OFFSET_STORE_TABLE))){
      val chengduGpsOffset = new HTableDescriptor(TableName.valueOf(ForeoConstants.HBASE_OFFSET_STORE_TABLE))
      chengduGpsOffset.addFamily(new HColumnDescriptor(ForeoConstants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(chengduGpsOffset)
      admin.close();
    }
    val table = connection.getTable(TableName.valueOf(ForeoConstants.HBASE_OFFSET_STORE_TABLE))
    var myReturnValue:collection.Map[TopicPartition, Long] = new mutable.HashMap[TopicPartition,Long]()
    for(eachTopic <- topics){
      val get = new Get((group+":"+eachTopic).getBytes())
      val result: Result = table.get(get)
      val cells: Array[Cell] = result.rawCells()
      for(result <- cells){
        //列名  group:topic:partition
        val topicPartition: String = Bytes.toString( CellUtil.cloneQualifier(result))
        //列值 offset
        val offsetValue: String = Bytes.toString(CellUtil.cloneValue(result))
        //切割列名，获取 消费组，消费topic，消费partition
        val strings: Array[String] = topicPartition.split(":")
        val myStr = strings(2)
        //println(myStr)
        val partition =  new TopicPartition(strings(1),strings(2).toInt)
        myReturnValue += (partition -> offsetValue.toLong)
      }
    }
    table.close()
    myReturnValue
  }

  def loadHBaseData(sparkSession:SparkSession,conf:Configuration):DataFrame = {
    val context: SparkContext = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, ForeoConstants.HTAB_HAIKOU_ORDER)
    val scan = new Scan
    //ORDER_ID String,CITY_ID String,STARTING_LNG String,STARTING_LAT String
    scan.addFamily(Bytes.toBytes("f1"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT"))
    //添加scan
    conf.set(TableInputFormat.SCAN, convertScanToString(scan))
    val rddResult: RDD[(ImmutableBytesWritable, Result)] = context.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    //

    import sparkSession.implicits._

    val haiKouOrderRdd: RDD[HaiKouOrderDo] = rddResult.mapPartitions(eachPartition => {
      val orders: Iterator[HaiKouOrderDo] = eachPartition.map(eachResult => {
        val value: Result = eachResult._2
        val orderId: String = Bytes.toString(value.getValue(Bytes.toBytes(ForeoConstants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID")))
        val cityId: String = Bytes.toString(value.getValue(Bytes.toBytes(ForeoConstants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID")))
        val startLng: String = Bytes.toString(value.getValue(Bytes.toBytes(ForeoConstants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG")))
        val startLat: String = Bytes.toString(value.getValue(Bytes.toBytes(ForeoConstants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT")))
        HaiKouOrderDo(orderId, cityId, startLng, startLat)
      })
      orders
    })
    haiKouOrderRdd.toDF
  }

  def convertScanToString(scan: Scan):String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def getHbaseConnection: Connection = {
    try{
      val config:Configuration = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum" , "hbase.zookeeper.quorum")
      //   config.set("hbase.master" , GlobalConfigUtils.getProp("hbase.master"))
      config.set("hbase.zookeeper.property.clientPort" , "hbase.zookeeper.property.clientPort")
      val connection = ConnectionFactory.createConnection(config)
      connection

    }catch{
      case exception: Exception => exception.getMessage
        null
    }
  }

}
