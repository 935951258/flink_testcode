package com.team.dosomething

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class TxEvent( txId: String, payChannel: String, eventTime: Long )
//case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

object OrderTxAnalysisWithIntervalJoin {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val datas: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\ReceiptLog.csv")
    val orderDS: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\OrderLog.csv")

    val orderDataDS: DataStream[OrderEvent] = orderDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        OrderEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    ).assignAscendingTimestamps(_.eventTime * 1000L)

    val txDS: DataStream[TxEvent] = datas.map(
      line => {
      val strings: Array[String] = line.split(",")
      TxEvent(
        strings(0),
        strings(1),
        strings(2).toLong
      )
    }).assignAscendingTimestamps(_.eventTime * 1000L)

      val orderKS: KeyedStream[OrderEvent, String] = orderDataDS.keyBy(_.txId)
      val txKS: KeyedStream[TxEvent, String] = txDS.keyBy(_.txId)

    //3.
    val resultDS: DataStream[String] = orderKS.intervalJoin(txKS)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(
        new ProcessJoinFunction[OrderEvent, TxEvent, String] {
          //两条流之间的处理逻辑
          /**
            *
            * @param left  第一条流的数据：Order
            * @param right 第二条流的数据：Tx
            * @param ctx   上下文
            * @param out   采集器
            */
          override def processElement(left: OrderEvent, right: TxEvent, ctx: ProcessJoinFunction[OrderEvent, TxEvent, String]#Context, out: Collector[String]) = {
            if (left.txId == right.txId) {
              out.collect("订单" + left.orderId + "对账成功")
            }

          }
        }
      )
    resultDS.print()

    environment.execute()
  }
}
