package com.chungjunming.online.streaming.util

import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * Created by Chungjunming on 2019/11/4.
  */
object Ip2Region {
  def main(args: Array[String]): Unit = {
    val ipSearch = new DbSearcher(new DbConfig(),this.getClass.getResource("/ip2region.db").getPath)
    val region = ipSearch.binarySearch("121.77.192.102").getRegion

    print(region)
  }
}
