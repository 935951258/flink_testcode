package com.team.dosomething

import java.sql.Timestamp
import java.util

import com.team.dosomething
import com.team.function.SimplePreAggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

case class HotAdClick(province:String,adId:Long,clickCount:Long,windowEnd:Long)
case class AdClickLog(
                       userId: Long,
                       adId: Long,
                       province: String,
                       city: String,
                       timestamp: Long)

/**
  * 热门广告点击
  */
object AdClickAnalysis {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val datas: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\AdClickLog.csv")

    val dataDS: DataStream[AdClickLog] = datas.map(line => {
      val strings: Array[String] = line.split(",")

      AdClickLog(
        strings(0).toLong,
        strings(1).toLong,
        strings(2),
        strings(3),
        strings(4).toLong
      )
    }).assignAscendingTimestamps(_.timestamp * 1000L)

    //通过 用户id和 广告id分组
    //这样分组可以不改变结构
    val provinceAnddataDS: KeyedStream[AdClickLog, (String, Long)] = dataDS.keyBy(data => {
      (data.province, data.adId)
    })
    val winEndKS: KeyedStream[HotAdClick, Long] = provinceAnddataDS
      .timeWindow(Time.hours(1), Time.minutes(5))
      //预聚合在通过 窗口结束时间分组
      .aggregate(
      new SimplePreAggregateFunction[AdClickLog],
      new ProcessWindowFunction[Long, HotAdClick, (String, Long), TimeWindow] {
        override def process(key: (String, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick]): Unit = {
          out.collect(dosomething.HotAdClick(key._1, key._2, elements.iterator.next(), context.window.getEnd))
        }
      }
    )
      .keyBy(_.windowEnd)


    val valueDS: DataStream[String] = winEndKS.process(
      new KeyedProcessFunction[Long, HotAdClick, String] {
        private var datalist: ListState[HotAdClick] = _
        private var triggerTs: ValueState[Long] = _


        override def open(parameters: Configuration) = {
          datalist = getRuntimeContext.getListState(new ListStateDescriptor[HotAdClick]("datalist", classOf[HotAdClick]))
          triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTS", classOf[Long]))

        }

        override def processElement(value: HotAdClick, ctx: KeyedProcessFunction[Long, HotAdClick, String]#Context, out: Collector[String]) = {
          //存数据
          datalist.add(value)
          //用定时器，模拟窗口的触发
          if (triggerTs.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd)
            triggerTs.update(value.windowEnd)
          }
        }

        //定时器触发操作
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotAdClick, String]#OnTimerContext, out: Collector[String]) = {
          val datas: util.Iterator[HotAdClick] = datalist.get().iterator()

          val listBuffer = new ListBuffer[HotAdClick]

          while (datas.hasNext) {
            listBuffer.append(datas.next())
          }

          datalist.clear()
          triggerTs.clear()

          val top3: ListBuffer[HotAdClick] = listBuffer.sortWith(_.clickCount > _.clickCount).take(3)
          out.collect(

            s"""
              |窗口结束时间：${new Timestamp(timestamp)}
               =======================================
               ${top3.mkString("\n")}
            """.stripMargin

          )

        }
      }
    )
    valueDS.print()


    environment.execute()
  }

}
