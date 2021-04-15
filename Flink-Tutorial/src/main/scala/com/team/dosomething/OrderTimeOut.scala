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
  *   OrderEvent样例类
  * @param orderId
  * @param eventType
  * @param txId
  * @param eventTime
  */
case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

/**
  * 订单支付实时监控
  */
object OrderTimeOut {

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
    //
    val unit: DataStream[String] = orderKS.process(
      new KeyedProcessFunction[Long, OrderEvent, String] {

        private var payEvent: ValueState[OrderEvent] = _
        private var createEvent: ValueState[OrderEvent] = _
        //定时器防止重复注册
        private var triggerTS: ValueState[Long] = _

        override def open(parameters: Configuration) = {

          //上一次的支付状态
          payEvent = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payEvent", classOf[OrderEvent]))
          createEvent = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("createEvent", classOf[OrderEvent]))
          triggerTS = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTS", classOf[Long]))


        }

        //触发操作，说明另一条数据没来
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]) = {
        //判断是谁没来
          if (payEvent.value() != null){
            //pay来过，create，，没来
            ctx.output(new OutputTag[String]("timeout"),"订单"+payEvent.value().orderId + "有支付数据，但create数据丢失，数据异常！")
            payEvent.clear()
          }
          if(createEvent.value()!=null){
            //create来过，pay没来
            ctx.output(new OutputTag[String]("timeout"),"订单"+createEvent.value().orderId+"超时，用户未支付")
            createEvent.clear()
          }
          //清空保存的定时时间
          triggerTS.clear()

        }

        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]) = {

          //每个订单的第一条数据来的时候，应该注册一个定时器
          if(triggerTS.value() == 0){
            //不管来的是 create还是pay，没必要做区分，直接等15分钟 ，不来，要么异常 ，要么超时
            ctx.timerService().registerEventTimeTimer(ctx.timerService().currentProcessingTime() + 15 * 60 *1000L)
            triggerTS.update(ctx.timerService().currentProcessingTime() + 15 * 60 *1000L)
          }else{
            //表示如果来的不是本订单的第一条数据，表示create和pay都到了，那么可以删除定时器
            ctx.timerService().deleteEventTimeTimer(triggerTS.value())
            triggerTS.clear()
          }



          //来的数据可能是create 也可能是 pay
          if ("create" == value.eventType) {
            //来的状态是 create
            //判断pay来过没
            if (null == payEvent.value()) {
              // pay状态没来过 =》 把create保存起来 ，等待 pay来
              createEvent.update(value)
            } else {
              //pay 状态来过,判断是否超时
              if (payEvent.value().eventTime - value.eventTime > 15 * 60) {

                //测输出流告警
                ctx.output(new OutputTag[String]("timeout"), value.orderId + "支付成功,但是超时，请检查业务系统是否存在异常")

              } else {
                out.collect("订单正常" + value.orderId + "正常")
              }
              payEvent.clear()
            }

          } else if ("pay" == value.eventType) {
            //来的数据是 pay =》 判断是creat 状态是否来过
            if (null == createEvent.value()) {
              payEvent.update(value)
            } else {
              //create状态已经来过,判断是否超时
              if (value.eventTime - createEvent.value().eventTime > 15 * 60) {

                //测输出流告警
                ctx.output(new OutputTag[String]("timeout"), value.orderId + "支付成功，但是超时，请检查业务系统是否存在异常")

              } else {
                out.collect("订单正常" + value.orderId + "正常")
              }
              createEvent.clear()
            }

          } else {
            out.collect("类型异常")
          }

        }
      }
    )
    unit.getSideOutput(new OutputTag[String]("timeout")).print("timeout")
    unit.print("result")


    environment.execute()

  }
}