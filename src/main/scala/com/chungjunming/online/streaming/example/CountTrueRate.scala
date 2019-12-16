package com.chungjunming.online.streaming.example

import java.sql.ResultSet

import com.chungjunming.online.streaming.bean.User2PointCorrectRate
import com.chungjunming.online.streaming.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by Chungjunming on 2019/11/4.
  */
object CountTrueRate {
  private val groupid = "rate_group_test"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val sparkContext = ssc.sparkContext
    val topics = Array("qz_log")
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    sparkContext.hadoopConfiguration.set("fs.defaultFS", "hdfs://nameservice1")
    sparkContext.hadoopConfiguration.set("dfs.nameservices", "nameservice1")
    ssc.checkpoint("/user/atguigu/sparkstreaming/checkpoint")

    val sqlProxy = new SqlProxy()
    val offsetMap = mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection

    try {
      sqlProxy.executeQuery(client, "select * from `offset_manager` where groupid=?", Array(groupid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            val model = new TopicPartition(rs.getString(2), rs.getInt(3))
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
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }

    val resultStream = stream.filter(item => item.value().split("\t").length == 6)
      .mapPartitions(partitions => {
        partitions.map(item => {
          //(用户id) (课程id) (知识点id) (题目id) (是否正确 0错误 1正确)(创建时间)
          val line = item.value()
          val arr = line.split("\t")
          val userId = arr(0)
          val courseId = arr(1)
          val pointId = arr(2)
          ((userId, courseId, pointId), 1)
        })
      }).reduceByKey(_ + _)
      .map {
        case ((userId, courseId, point), count) => (courseId, count)
      }
    //    resultStream.print()

    val acc = new MyAcc
    ssc.sparkContext.register(acc)

    val resultStream2 = stream.filter(item => item.value().split("\t").length == 6)
      .mapPartitions(partitions => {
        partitions.map(item => {
          //(用户id) (课程id) (知识点id) (题目id) (是否正确 0错误 1正确)(创建时间)
          val line = item.value()
          val arr = line.split("\t")
          val userId = arr(0)
          val courseId = arr(1)
          val pointId = arr(2)
          val questionId = arr(3)
          val isTrue = arr(4)
          val createtime = arr(5)
          User2PointCorrectRate(userId, courseId, pointId, questionId, isTrue, createtime)
        })
      })
    resultStream2.foreachRDD(rdd => {
      rdd.foreach(
        user =>
          acc.add(user)
      )
    })

    acc.map.foreach(println)

    stream.foreachRDD(
      rdd => {
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          for (or <- offsetRanges) {
            sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
              Array(groupid, or.topic, or.partition.toString, or.untilOffset))
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          sqlProxy.shutdown(client)
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

class MyAcc extends AccumulatorV2[User2PointCorrectRate, Map[String, Double]] {
  var map = Map[String, Double]()

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[User2PointCorrectRate, Map[String, Double]] = {
    val acc = new MyAcc
    acc.map ++= map
    acc
  }

  override def reset(): Unit = map = Map[String, Double]()

  override def add(v: User2PointCorrectRate): Unit = {
    if (v.isTrue.toInt == 1) {
      map += ("correctCount") -> (map.getOrElse("correctCount", 0D) + 1D)
    }
    map += ("totalCount") -> (map.getOrElse("totalCount", 0D) + 1D)
  }

  override def merge(other: AccumulatorV2[User2PointCorrectRate, Map[String, Double]]): Unit = {
    other match {
      case o: MyAcc =>
        this.map += "correctCount" -> ((this.map.getOrElse("correctCount", 0D)) + o.map.getOrElse("correctCount", 0D))
        this.map += "totalCount" -> ((this.map.getOrElse("totalCount", 0D)) + o.map.getOrElse("totalCount", 0D))
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Double] = {
    this.map += "correctRate" -> this.map.getOrElse("correctCount", 0D) / this.map.getOrElse("totalCount", 1D)
    this.map
  }
}