package com.chungjunming.online.streaming.example

import java.sql.{Connection, ResultSet}
import java.time.{LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter

import com.chungjunming.online.streaming.bean.LearnModel
import com.chungjunming.online.streaming.util.{DataSourceUtil, QueryCallback, SqlProxy}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by Chungjunming on 2019/11/5.
  */
object QzPointStreaming {
  private val groupid = "qz_point_group"

  val map = new mutable.HashMap[String, LearnModel]()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
    //      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    val scc = new StreamingContext(conf, Seconds(3))
    val topic = Array("qz_log")
    val kafkaMap = Map[String, Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
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

    /*    val stream = if (offsetMap.isEmpty) {
          KafkaUtils.createDirectStream(scc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap))
        } else {
          KafkaUtils.createDirectStream(scc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap, offsetMap))
        }*/
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(
        scc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(
        scc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topic, kafkaMap, offsetMap))
    }

    val dsStream = stream.filter(item => item.value().split("\t").length == 6)
      .mapPartitions(partitions => {
        partitions.map { item =>
          val line = item.value()
          val arr = line.split("\t")
          val uid = arr(0) //用户id
        val courseid = arr(1) //课程id
        val pointid = arr(2) //知识点id
        val questionid = arr(3) //题目id
        val istrue = arr(4) //是否正确
        val createtime = arr(5) //创建时间
          (uid, courseid, pointid, questionid, istrue, createtime)
        }
      })

    /*    dsStream.foreachRDD(
          rdd => {
            val groupRdd = rdd.groupBy(item => item._1 + "_" + item._2 + "_" + item._3)
            groupRdd.foreachPartition(partitions => {
              val sqlProxy = new SqlProxy()
              val client = DataSourceUtil.getConnection
              try {
                partitions.foreach {
                  case (key, iters) =>
                    qzQuestionUpdate(key, iters, sqlProxy, client)
                }
              } catch {
                case e: Exception => e.printStackTrace()
              } finally {
                sqlProxy.shutdown(client)
              }
            })
          }
        )*/
    dsStream.foreachRDD(rdd => {
      //获取相同用户 同一课程 同一知识点的数据
      val groupRdd = rdd.groupBy(item => item._1 + "-" + item._2 + "-" + item._3)
      groupRdd.foreachPartition(partition => {
        //在分区下获取jdbc连接
        val sqlProxy = new SqlProxy()
        val client = DataSourceUtil.getConnection
        try {
          partition.foreach { case (key, iters) =>
            qzQuestionUpdate(key, iters, sqlProxy, client) //对题库进行更新操作
          }
        } catch {
          case e: Exception => e.printStackTrace()
        }
        finally {
          sqlProxy.shutdown(client)
        }
      }
      )
    })


    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
      try {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (or <- offsetRanges) {
          /*          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
                      Array(groupid, or.topic, or.partition.toString, or.untilOffset))*/
          sqlProxy.executeUpdate(client, "replace into `offset_manager` (groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
            Array(groupid, or.topic, or.partition.toString, or.untilOffset))
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    })

    scc.start()
    scc.awaitTermination()
  }

  def qzQuestionUpdate(key: String, iters: Iterable[(String, String, String, String, String, String)], sqlProxy: SqlProxy, client: Connection) = {
    val keys = key.split("-")
    val userid = keys(0).toInt
    val courseid = keys(1).toInt
    val pointid = keys(2).toInt
    val array = iters.toArray
    val questionids = array.map(_._4).distinct
    var questionids_history: Array[String] = Array()

    sqlProxy.executeQuery(client, "select questionids from `qz_point_history` where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            questionids_history = rs.getString(1).split(",")
          }
          rs.close()
        }
      })

    val resultQuestionid = questionids.union(questionids_history).distinct
    val countSize = resultQuestionid.length
    val resultQuestionid_str = resultQuestionid.mkString(",")
    val qz_count = questionids.length
    var qz_sum = array.length
    var qz_istrue = array.filter(_._5.equals("1")).size
    val createtime = array.map(_._6).min
    val updatetime = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss").format(LocalDateTime.now())

      sqlProxy.executeUpdate(client, "insert into qz_point_history(userid,courseid,pointid,questionids,createtime,updatetime) " +
      "values(?,?,?,?,?,?) on duplicate key update questionids =?,updatetime=?", Array(userid, courseid, pointid, resultQuestionid_str, createtime, updatetime, resultQuestionid_str, updatetime))

    var qzSum_history = 0
    var istrue_history = 0

    sqlProxy.executeQuery(client, "select qz_sum,qz_istrue from qz_point_detail where userid=? and courseid=? and pointid=?",
      Array(userid, courseid, pointid), new QueryCallback {
        override def process(rs: ResultSet): Unit = {
          while (rs.next()) {
            qzSum_history += rs.getInt(1)
            istrue_history += rs.getInt(2)
          }
          rs.close()
        }
      })
    qz_sum += qzSum_history
    qz_istrue += istrue_history
    val correct_rate = qz_istrue.toDouble / qz_sum.toDouble

    val qz_detail_rate = countSize.toDouble / 30
    val mastery_rate = qz_detail_rate * correct_rate
    //    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
    //      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate,updatetime=?",
    //      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))
    sqlProxy.executeUpdate(client, "insert into qz_point_detail(userid,courseid,pointid,qz_sum,qz_count,qz_istrue,correct_rate,mastery_rate,createtime,updatetime)" +
      " values(?,?,?,?,?,?,?,?,?,?) on duplicate key update qz_sum=?,qz_count=?,qz_istrue=?,correct_rate=?,mastery_rate=?,updatetime=?",
      Array(userid, courseid, pointid, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, createtime, updatetime, qz_sum, countSize, qz_istrue, correct_rate, mastery_rate, updatetime))


  }
}
