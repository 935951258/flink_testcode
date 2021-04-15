package com.team.apiTest

import java.util.Properties

import org.apache.flink.streaming.api.scala._


//样例类
case class WaterSensor(id: String, timestamp: Long , vc: Double)

object SourceTest2 {
def main(args: Array[String]): Unit = {

    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    val stream: DataStream[WaterSensor] = environment.fromCollection(
      List(
        WaterSensor("ws_001", 1577844001, 45.0),
        WaterSensor("ws_002", 1577844022, 43.0),
        WaterSensor("ws_003", 1577844033, 42.0),
        WaterSensor("ws_004", 1577844044, 41.0),
        WaterSensor("ws_005", 1577844055, 40.0),
        WaterSensor("ws_006", 1577844066, 46.0)
      ))

    //ws_001,1577844001,45.0
    val dataDS: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\data.txt")

    //dataDS.keyBy()

    environment.execute()
  }
}
