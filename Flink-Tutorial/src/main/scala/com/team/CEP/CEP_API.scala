package com.team.CEP

import com.team.apiTest.WaterSensor
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


object CEP_API {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sensorDS: DataStream[WaterSensor] = environment.readTextFile("Data/data.txt")
      .map(line => {
        val strings: Array[String] = line.split(",")
        WaterSensor(
          strings(0),
          strings(1).toLong,
          strings(2).toDouble
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

      // TODO CEP
      //1.定义规则
    val pattern: Pattern[WaterSensor, WaterSensor] = Pattern
      .begin[WaterSensor]("start")
      .where(_.id == "\"ws_006\"")


      //2.应用规则 把规则应用到 数据流中
      //  第一个参数：要匹配规则的数据流
      //  第二个参数：定义的规则
      val sensorPS: PatternStream[WaterSensor] = CEP.pattern(sensorDS,pattern)

      //3.获取匹配的结果
    val resultDS: DataStream[String] = sensorPS.select(
      data => {
        data.toString()
      }
    )

    resultDS.print("cep")

      environment.execute()
  }
}
