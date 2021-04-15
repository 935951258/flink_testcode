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

case class HotAdClick1(userID:Long,adId:Long,clickCount:Long,windowEnd:Long)
/*case class AdClickLog(
                       userId: Long,
                       adId: Long,
                       province: String,
                       city: String,
                       timestamp: Long)*/
/**
  * 黑名单检测
  */
object BlackList {

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
    val userAnddataDS: KeyedStream[AdClickLog, (Long, Long)] = dataDS.keyBy(data => {
      (data.userId, data.adId)
    })

    val resultDS: DataStream[AdClickLog] = userAnddataDS.process(
      new BlackListFilter
    )


    //测输出流告警
    resultDS.getSideOutput(new OutputTag[String]("alarm")).print("black")

    //topN
    resultDS
      .keyBy(data=>{(data.userId,data.adId)})
        .timeWindow(Time.hours(1),Time.minutes(5))
        .aggregate(
          new SimplePreAggregateFunction[AdClickLog],
          new ProcessWindowFunction[Long,HotAdClick1,(Long,Long),TimeWindow] {
            override def process(key: (Long, Long), context: Context, elements: Iterable[Long], out: Collector[HotAdClick1]): Unit = {
              out.collect(HotAdClick1(key._1,key._2,elements.iterator.next(),context.window.getEnd))
            }
          }
        )
        .keyBy(_.windowEnd)
        .process(
          new KeyedProcessFunction[Long,HotAdClick1,String] {
            override def processElement(value: HotAdClick1, ctx: KeyedProcessFunction[Long, HotAdClick1, String]#Context, out: Collector[String]) = {
                //topN 代码
            }
          }
        )






    environment.execute()
  }

}

/**
  * 计算出晚上12点的事件戳 ，并当作定时器的时间，定时器触发，把点击总次数和 trigger 清空
  * 定义一个，值状态保存，总共的累计次数，
  *  如果某用户对某商品点击次数超过100 ，则用测输出流报警，
  *  没有超过则 点击次数+1
  *
  *
  */
class BlackListFilter extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog] {
    //定义一个键控状态，保存点击量累计值
    private var clickCount:ValueState[Int] = _
    //防止重复注册定时器
    private var trigger: Long = 0L
    private var alarmFlag:ValueState[Boolean] = _

  //调用生命周期方法，初始化 键控状态
  override def open(parameters: Configuration): Unit = {
      //初始化键控状态
    clickCount = getRuntimeContext.getState(new ValueStateDescriptor[Int]("clickCount",classOf[Int]))

    alarmFlag = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("alarmFlag",classOf[Boolean]))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //定时器触发 ，说明已经到了隔天的 0 点
    clickCount.clear()

    trigger = 0L
  }

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    var currentCount: Int = clickCount.value()

    //如果点击量为0，则说明是今天第一次点击，也就是说这条数据属于今天
    if(currentCount == 0){
      //考虑晚上24点，对count值进行清0， =》 用定时器实现 =》需要获取隔天的时间戳
      //1.取整， 去当天0点的事件戳
      //一天的毫秒值
      val aDayTimeOfMillisecond: Long = 24 * 60 * 60 * 1000L
      //时间戳 / 一天的毫秒值 = 共计的天数
      val currentDay: Long = value.timestamp * 1000L / aDayTimeOfMillisecond
      // （共计天数 + 1 ）* 一天的毫秒值 = 第二天的 0点毫秒值
      val nextDayMillisend: Long = (currentDay + 1) * aDayTimeOfMillisecond

      //注册定时器
      //trigger
      if (trigger == 0){
        //正常是当前时间
        //ctx.timerService().registerProcessingTimeTimer(nextDayMillisend)
        ctx.timerService().registerEventTimeTimer(nextDayMillisend)
        trigger = nextDayMillisend
      }
    }


    //如果点击量的键控状态超过100 则用测输出流告警
    if (currentCount >= 100){
      //如果已经告警，那么不在频繁告警
      // alarmFlag 默认为false
      if (!alarmFlag.value()){
        val alarmTag = new OutputTag[String]("alarm")
        ctx.output(alarmTag,"用户："+value.userId+"对广告:"+value.adId+"点击戳阈值，可能存在恶意刷单")
        alarmFlag.update(true)
      }
    }else{
        clickCount.update(currentCount + 1)
        out.collect(value)
    }

  }
}