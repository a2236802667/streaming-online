package com.chungjunming.online.streaming.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by Chungjunming on 2019/11/4.
  */
object CourseLearnProducer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("course-Producer").setMaster("local[*]")
    val ssc = new SparkContext(sparkConf)
    ssc.hadoopConfiguration.set("fs.defaultFS","file://")
//    System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-2.7.2")
    val resultLog = ssc.textFile(this.getClass.getResource("/course_learn.log").toURI.getPath, 10)
      .foreachPartition(partitoin => {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop001:9092,hadoop002:9092,hadoop003:9092")
        props.put("acks", "1")
        props.put("batch.size", "16384")
        props.put("linger.ms", "10")
        props.put("buffer.memory", "33554432")
        props.put("key.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer",
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partitoin.foreach(item => {
          val msg = new ProducerRecord[String, String]("course_learn", item)
          producer.send(msg)
        })
        producer.flush()
        producer.close()
      })
  }
}
