package com.chungjunming.online.streaming.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.chungjunming.online.streaming.bean.{Register, RegisterCount}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.util.AccumulatorV2

/**
  * Created by Chungjunming on 2019/11/4.
  */
object StaticRegisterInfoService {
  def getRegisterCount(ssc: StreamingContext, inputDStream: InputDStream[ConsumerRecord[String, String]]) = {
    ssc.sparkContext.setCheckpointDir("./checkpoint")
    val registerLogStream = inputDStream.map(record => {
      val splits = record.value().split("\t")
      //      (Register(splits(0), splits(1), splits(2)),1)
      Register(splits(0), splits(1), splits(2))
    }).map((1,_))

    val registerLogStream2 = inputDStream.map(record => {
      val splits = record.value().split("\t")
      //      (Register(splits(0), splits(1), splits(2)),1)
      Register(splits(0), splits(1), splits(2))
    }).map((_,1))

//    registerLogStream2.cache()

    val resultStream = registerLogStream.updateStateByKey[(Int, Int)] {
      (seq: Seq[Register], opt: Option[(Int, Int)]) =>
        Some(seq.size,seq.size + opt.getOrElse((0,0))._1)
    }.map(
      x => (x._2)
    )

//    registerLogStream2.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,Seconds(60),Seconds(6))
//        .print()

//    resultStream.print()

/*
    registerLogStream2.updateStateByKey[Int]({
      (seq:Seq[Int],opt:Option[(Int)]) => {
        Some(seq.sum + opt.getOrElse(0))
      }
    }).print()
*/
val resultDStream = inputDStream.filter(item => item.value().split("\t").length == 3).
  mapPartitions(partitions => {
    partitions.map(item => {
      val line = item.value()
      val arr = line.split("\t")
      val app_name = arr(1) match {
        case "1" => "PC"
        case "2" => "APP"
        case _ => "Other"
      }
      (app_name, 1)
    })
  })
    resultDStream.cache()
    resultDStream.reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(6)).print()


  }

}
