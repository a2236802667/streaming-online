package com.chungjunming.online.streaming.controller

import com.chungjunming.online.streaming.service.StaticRegisterInfoService
import com.chungjunming.online.streaming.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Chungjunming on 2019/11/4.
  */
object StaticRegisterInfoController {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("static register info").setMaster("local[1]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val inputDStream = MyKafkaUtil.getKafkaStream("register_topic",ssc)

    StaticRegisterInfoService.getRegisterCount(ssc,inputDStream)

    ssc.start()
    ssc.awaitTermination()
//    sss
  }
}
