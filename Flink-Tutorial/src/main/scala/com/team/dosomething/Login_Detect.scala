package com.team.dosomething

import java.sql.Timestamp
import java.util

import com.team.dosomething
import com.team.function.SimplePreAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
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
  * 恶意登陆样例类
  * @param userId
  * @param ip
  * @param eventType
  * @param eventTime
  */
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

/**
  * 恶意登陆
  */
object Login_Detect {

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

    //过滤出 eventType 为 fail的数据
    val failDatas: DataStream[LoginEvent] = datasDS.filter(_.eventType == "fail")

    //通过 userid 进行分组
    val loginKS: KeyedStream[LoginEvent, Long] = failDatas.keyBy(_.userId)

    val unit: DataStream[String] = loginKS.process(
      new KeyedProcessFunction[Long, LoginEvent, String] {

        //上次失败的状态
        private var lastLoginFail: ValueState[LoginEvent] = _

        override def open(parameters: Configuration) = {

          //存一个上一次失败的状态
          lastLoginFail = getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("lastLoginFail", classOf[LoginEvent]))
        }

        override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]) = {

          if ("fail" == value.eventType) {
            //判断是否为第一条失败数据
            if (lastLoginFail.value() == null) {
              lastLoginFail.update(value)
            } else {
              //说明不是第一条失败数据，那么就要判断 两次失败的间隔时间(考虑乱序，所以要取绝对值)
              if (Math.abs(value.eventTime - lastLoginFail.value().eventTime) <= 2L) {
                out.collect("用户：" + value.userId + "在两秒连续登陆失败！可能为恶意登陆")

                /**
                  * 存在问题
                  * 如果两秒内多次失败呢? 10次呢 用ListState？
                  * 代码中并没有保证两次失败是连续的？ 中间隔了成功呢？
                  * 数据是乱序的，应该是F,S,F， 但是数据来的顺序是F,F,S 那么会不会误判
                  */

              }
            }
          }

        }
      }
    )
    unit.print()



      environment.execute()
  }
}