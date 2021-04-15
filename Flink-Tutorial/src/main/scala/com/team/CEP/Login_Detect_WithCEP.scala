package com.team.CEP

import java.sql.Timestamp
import java.util

import com.team.dosomething
import com.team.dosomething.LoginEvent
import com.team.function.SimplePreAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



/**
  * 恶意登陆 CEP 实现
  */
object Login_Detect_WithCEP {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val datas: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\LoginLog.csv")

    val datasDS: DataStream[LoginEvent] = datas.map(line => {
      val strings: Array[String] = line.split(",")

      LoginEvent(
        strings(0).toLong,
        strings(1),
        strings(2),
        strings(3).toLong
      )
    }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime
      }
    )

   //使用 CEP编程来 实现
    //1.定义规则
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern
      //连续两秒类登录失败的规则
      .begin[LoginEvent]("start").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    //2.应用规则
    val loginPS: PatternStream[LoginEvent] = CEP.pattern(datasDS,pattern)

    //3.获取匹配规则的结果
    val value: DataStream[String] = loginPS.select(
      data => {
        data.toString()
      }
    )
    value.print("---")



      environment.execute()
  }
}