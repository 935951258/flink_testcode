package com.team.CEP

import java.sql.Timestamp
import java.util

import com.team.dosomething
import com.team.dosomething.OrderEvent
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
  * 订单支付实时监控
  */
object OrderTimeOut_WithCEP {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val datas: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\OrderLog.csv")

    val datasDS: DataStream[OrderEvent] = datas.map(line => {
      val strings: Array[String] = line.split(",")

      OrderEvent(
        strings(0).toLong,
        strings(1),
        strings(2),
        strings(3).toLong
      )
    }).assignAscendingTimestamps(_.eventTime * 1000L)

    //处理数据
    //按照统计的维度分组： 订单
    val orderKS: KeyedStream[OrderEvent, Long] = datasDS.keyBy(_.orderId)

    //使用CEP 实现超时监控
    //局限性：  没法发现 超时但是支付的情况 但是一般这种情况  本身就是异常情况
    //          没法发现数据丢失 但是这种情况 本身就是异常情况

    //1.定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //2.使用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS,pattern)

    //3.获取匹配规则的数据
    //select 可以传三个参数
        //第一个参数：侧输出流的标签对象 =》 侧输出流 用来放 超时数据的处理结果
        //第二个参数： 对超时数据的 处理 =》 处理完的数据，放入侧输出流
        //第三个参数： 对匹配上的数据的处理
      //三个参数可以通过柯里化的方式传参
    val timeout = new OutputTag[String]("timeout")
    val result: DataStream[String] = orderPS.select(timeout)(
      (timeoutData, ts) => {
        timeoutData.toString()
      }
    )(
      data => {
        data.toString()
      }
    )

    val timeoutDs: DataStream[String] = result.getSideOutput(new OutputTag[String]("timeout"))

    timeoutDs.print()

    environment.execute()

  }
}