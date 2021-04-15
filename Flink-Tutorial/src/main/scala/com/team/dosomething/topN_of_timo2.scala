package com.team.dosomething


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

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

import scala.collection.mutable.ListBuffer

/**
  * 页面浏览样例类
  * @param ip
  * @param userId
  * @param eventTime
  * @param method
  * @param url
  */
case class ApacheLog(
                      ip:String,
                      userId:String,
                      eventTime:Long,
                      method:String,
                      url:String)

object topN_of_timo2 {
  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val logDs: DataStream[String] = environment.readTextFile("Data/apache.log")
    //3.转换为样例类
    val apacheLogDS: DataStream[ApacheLog] = logDs.map(data => {
      val stringsArray: Array[String] = data.split(" ")

      //转换格式
      val date = new SimpleDateFormat("dd/MM/yyyy:HH:ss")
      val time: Long = date.parse(stringsArray(3)).getTime

      ApacheLog(
        stringsArray(0).toString,
        stringsArray(1).toString,
        time,
        stringsArray(5).toString,
        stringsArray(6).toString
      )
    }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLog): Long = {
          element.eventTime
        }
      }
    )

    //过滤png格式
    //apacheLogDS.filter()

    //处理数据
    val apacheLogKS: KeyedStream[ApacheLog, String] = apacheLogDS.keyBy(_.url)

    //开窗口
    val apacheLogWS: WindowedStream[ApacheLog, String, TimeWindow] = apacheLogKS.timeWindow(Time.minutes(10),Time.seconds(5))

    //第一个函数： 做预聚和操作，将结果传递给全窗口函数
    //第二个函数： 将预聚和的结果，加上 窗口结束时间 的标记，方便后面 按照窗口进行分组
    val aggDS: DataStream[HotPageView] = apacheLogWS.aggregate(
      new SimplePreAggregateFunction[ApacheLog],
      new MyProcessWindowFunction2
    )
    
    //按照窗口结束时间分组，为了将同一个窗口的数据放在一起排序
    val aggKS: KeyedStream[HotPageView, Long] = aggDS.keyBy(_.windowEnd)

    //进行排序
    val top3DS: DataStream[String] = aggKS.process(
      new KeyedProcessFunction[Long, HotPageView, String] {

        private var datalist: ListState[HotPageView] = _
        private var triggerTs: ValueState[Long] = _


        override def open(parameters: Configuration) = {
          //创建状态
          datalist = getRuntimeContext.getListState(new ListStateDescriptor[HotPageView]("datalist", classOf[HotPageView]))
          triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs", classOf[Long]))
        }

        override def processElement(value: HotPageView, ctx: KeyedProcessFunction[Long, HotPageView, String]#Context, out: Collector[String]) = {
          //存数据
          datalist.add(value)
          //用定时器模拟窗口操作
          if (triggerTs.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd)
            triggerTs.update(value.windowEnd)
          }
        }

        //定时器触发的操作，取topN
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotPageView, String]#OnTimerContext, out: Collector[String]) = {
          //1.从集合取出
          val datas: util.Iterator[HotPageView] = datalist.get().iterator()
          //2.放入scala的集合中
          val listBuffer = new ListBuffer[HotPageView]
          while (datas.hasNext) {

            listBuffer.append(datas.next())
          }

          //3.排序，取topN
          val top3: ListBuffer[HotPageView] = listBuffer.sortBy(_.clickCount)(Ordering.Long.reverse).take(3)

          //4.通过采集器发送
          out.collect(
            s"""
窗口结束时间：${new Timestamp(timestamp)}
${top3.mkString("\n")}

             """.stripMargin
          )
        }
      }
    )
      top3DS.print("top3")

    environment.execute()
  }

}

//全窗口函数，输入时 预聚和函数的输出
//给每个窗口的统计结果加上窗口结束时间的 标记
class MyProcessWindowFunction2 extends ProcessWindowFunction[Long,HotPageView,String,TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[HotPageView]): Unit = {
      out.collect(HotPageView(key,elements.iterator.next(),context.window.getEnd))
  }
}


case class HotPageView(url: String, clickCount: Long, windowEnd: Long)