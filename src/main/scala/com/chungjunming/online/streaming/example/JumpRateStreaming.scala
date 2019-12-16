package com.chungjunming.online.streaming.example

import java.sql.{Connection, ResultSet}
import java.text.NumberFormat

import com.chungjunming.online.streaming.util.{DataSourceUtil, ParseJsonData, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkFiles}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Chungjunming on 2019/11/5.
  */
//{"app_id":"1","device_id":"102","distinct_id":"5fa401c8-dd45-4425-b8c6-700f9f74c532","event_name":"-",
// "ip":"121.76.152.135","last_event_name":"-","last_page_id":"0","
// next_event_name":"-","next_page_id":"2","page_id":"1","server_time":"-","uid":"245494"}
object JumpRateStreaming {
  private val groupid = "jump_rate_group"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)/*.setMaster("local[*]")*/
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext = ssc.sparkContext
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")

    val topic = Array("page_topic")
    val kafkaMap = Map[String, Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sqlProxy = new SqlProxy()
    val offsetMap = mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection

    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid =?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val topic = rs.getString(2)
            val partition = rs.getInt(3)
            val model = new TopicPartition(topic, partition)
            val offset = rs.getLong(4)
            offsetMap.put(model, offset)
          }
          rs.close()
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      sqlProxy.shutdown(client)
    }

    val stream = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap, offsetMap))
    }

    val dsStream = stream.map(item => item.value()).mapPartitions(partitions => {
      partitions.map(item => {
        val jsonObject = ParseJsonData.getJsonData(item)
        val uid = if (jsonObject.containsKey("uid")) jsonObject.getString("uid") else ""
        val app_id = if (jsonObject.containsKey("app_id")) jsonObject.getString("app_id") else ""
        val device_id = if (jsonObject.containsKey("device_id")) jsonObject.getString("device_id") else ""
        val ip = if (jsonObject.containsKey("ip")) jsonObject.getString("ip") else ""
        val last_page_id = if (jsonObject.containsKey("last_page_id")) jsonObject.getString("last_page_id") else ""
        val pageid = if (jsonObject.containsKey("page_id")) jsonObject.getString("page_id") else ""
        val next_page_id = if (jsonObject.containsKey("next_page_id")) jsonObject.getString("next_page_id") else ""
        (uid, app_id, device_id, ip, last_page_id, pageid, next_page_id)
      })
    })

    dsStream.cache()

    val pageValueDStream = dsStream.map(item => (item._5 + "_" + item._6 + "_" + item._7, 1))
    val resultDStream = pageValueDStream.reduceByKey(_ + _)
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach(
            item => {
              calcPageJumpCount(sqlProxy, item, client)
            }
          )
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      })
    })

        ssc.sparkContext.addFile("hdfs://nameservice1/user/atguigu/sparkstreaming/ip2region.db")
//    ssc.sparkContext.addFile(this.getClass.getResource("/ip2region.db").getPath)

    val ipDStream = dsStream.mapPartitions(partitions => {
      val dbFile = SparkFiles.get("ip2region.db")
      val ipsearch = new DbSearcher(new DbConfig(), dbFile)
      partitions.map(item => {
        val ip = item._4
        val province = ipsearch.memorySearch(ip).getRegion.split("\\|")(2)
        (province, 1L)
      })
    }).reduceByKey(_ + _)

    ipDStream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val history_data = new ArrayBuffer[(String, Long)]()
        sqlProxy.executeQuery(client, "select province,num from tmp_city_num_detail", null, new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val tuple = (rs.getString(1), rs.getLong(2))
              history_data += tuple
            }
          }
        })
        val history_rdd = ssc.sparkContext.makeRDD(history_data)
        val resultRdd = history_rdd.fullOuterJoin(rdd).map(item => {
          val province = item._1
          val nums = item._2._1.getOrElse(0L) + item._2._2.getOrElse(0L)
          (province, nums)
        })
        resultRdd.foreachPartition(partitions => {
          val sqlProxy = new SqlProxy()
          val client = DataSourceUtil.getConnection
          try {
            partitions.foreach(item => {
              val province = item._1
              val num = item._2
              sqlProxy.executeUpdate(client, "insert into tmp_city_num_detail(province,num) values(?,?)" +
                " on duplicate key update num=?", Array(province, num, num))
            })
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            sqlProxy.shutdown(client)
          }
        })
        val top3Rdd = resultRdd.sortBy[Long](_._2, false).take(3)
        sqlProxy.executeUpdate(client, "truncate table top_city_num", null)
        top3Rdd.foreach(item => {
          sqlProxy.executeUpdate(client, "insert into top_city_num(province,num) values(?,?)", Array(item._1, item._2))
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })


    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        calcJumRate(sqlProxy, client)
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untiloffset)" +
            " values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def calcJumRate(sqlProxy: SqlProxy, client: Connection) = {
    var page1_num = 0L
    var page2_num = 0L
    var page3_num = 0L

    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id =?", Array(1), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page1_num += rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id =?", Array(2), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page2_num += rs.getLong(1)
        }
      }
    })
    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id =?", Array(3), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          page3_num += rs.getLong(1)
        }
      }
    })

    val nf = NumberFormat.getPercentInstance()
    val page1ToPage2Rate = if (page1_num == 0) "0%" else nf.format(page2_num.toDouble / page1_num.toDouble)
    val page2ToPage3Rate = if (page2_num == 0) "0%" else nf.format(page3_num.toDouble / page2_num.toDouble)

    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page1ToPage2Rate, 2))
    sqlProxy.executeUpdate(client, "update page_jump_rate set jump_rate=? where page_id=?", Array(page2ToPage3Rate, 3))
  }

  def calcPageJumpCount(sqlProxy: SqlProxy, item: (String, Int), client: Connection) = {
    val keys = item._1.split("_")
    var num:Long = item._2
    val page_id = keys(1).toInt
    val last_page_id = keys(0).toInt
    val next_page_id = keys(2).toInt

    sqlProxy.executeQuery(client, "select num from page_jump_rate where page_id =?", Array(page_id), new QueryCallback {
      override def process(rs: ResultSet): Unit = {
        while (rs.next()) {
          num += rs.getLong(1)
        }
        rs.close()
      }
    })

    if (page_id == 1) {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num,jump_rate)" +
        " values(?,?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, "100%", num))
    } else {
      sqlProxy.executeUpdate(client, "insert into page_jump_rate(last_page_id,page_id,next_page_id,num)" +
        " values(?,?,?,?) on duplicate key update num=?", Array(last_page_id, page_id, next_page_id, num, num))
    }


  }
}
