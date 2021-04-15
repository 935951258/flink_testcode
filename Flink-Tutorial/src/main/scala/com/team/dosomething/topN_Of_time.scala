package com.team.dosomething

import java.sql.Timestamp
import java.{lang, util}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object topN_Of_time {

  def main(args: Array[String]): Unit = {
    
  
   //1.创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val logDs: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\Data\\UserBehavior.csv")
    //3.转换为样例类
    val userBehaviorDs: DataStream[UserBehavior] = logDs.map(
      line => {
        val stringsArray: Array[String] = line.split(",")
        UserBehavior(
          stringsArray(0).toLong,
          stringsArray(1).toLong,
          stringsArray(2).toInt,
          stringsArray(3),
          stringsArray(4).toLong
        )
      }
    )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //3.1处理数据
    //3.2过滤数据
     val filterDS: DataStream[UserBehavior] = userBehaviorDs.filter(_.behavior == "pv")

    /*filterDS
      .map(data=>(data.itemId,1L))
    .keyBy(_._1)
    .timeWindow(Time.hours(1),Time.minutes(5))
    //.sum(1)  滚动聚合 没法排序
    //.reduce() 滚动聚合 没法排序
    //.process()  可行  但是会攒数据 会有风险
      //增量聚合  窗口关闭时计算
    .aggregate()*/

  //通过 itemID 分组
   val userBehaviorKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

  //开窗口
   val userBehaviorWS: WindowedStream[UserBehavior, Long, TimeWindow] = userBehaviorKS.timeWindow(Time.hours(1),Time.minutes(5))

  //第一个函数：预聚和函数，每来一条数据就累加一次，直到窗口触发的时候才会把统计的
  //结果值 传递给 全窗口函数
  //第二个函数：全窗口函数，输入就是 预聚和函数的输出；
  //作用是将数据 打上结束时间这个标记，用于 后面在流中 区分来自不用窗口的数据类型
   val aggDS: DataStream[HotItemClik] = userBehaviorWS
    .aggregate(
    new MyaggregateFunction,
    new MyProcessWindowFunction
  )

  // 按照 窗口结束时间分组 ，将同一个窗口的统计值，汇总到一个分组里 ，方便进行排序
   val aggks: KeyedStream[HotItemClik, Long] = aggDS.keyBy(_.windowEndTime)

  // 排序
   val processDS: DataStream[String] = aggks.process( new MyKeyedProcessFunction)

  processDS.print("hot item")


  environment.execute()
}
}





class MyKeyedProcessFunction extends KeyedProcessFunction[Long,HotItemClik,String] {

  private var dataList:ListState[HotItemClik] =_
  private var triggerTs:ValueState[Long] =_


  override def open(parameters: Configuration): Unit = {

    //创建键控状态集合
    dataList = getRuntimeContext.getListState(new ListStateDescriptor[HotItemClik]("data list",classOf[HotItemClik]))

    //创建触发器
     triggerTs = getRuntimeContext.getState(new ValueStateDescriptor[Long]("triggerTs",classOf[Long]))
  }

  override def processElement(value: HotItemClik, ctx: KeyedProcessFunction[Long, HotItemClik, String]#Context, out: Collector[String]): Unit = {
    //排序 需要数据到期，这个方法数数据一条条的来，所以需要 先存起来
    //存到键控状态的 list中
    dataList.add(value)

    //设置定时器，在窗口结束时间，触发开始排序
    if (triggerTs.value() == 0){

      //防止重复创建计时器
      ctx.timerService().registerEventTimeTimer(value.windowEndTime)
      triggerTs.update(value.windowEndTime)
    }

  }


  //定时器触发 ，进行排序
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, HotItemClik, String]#OnTimerContext, out: Collector[String]): Unit = {
    //排序
    val datas: util.Iterator[HotItemClik] = dataList.get().iterator()
    val listBuffer = new ListBuffer[HotItemClik]

    //java List 转 scala List
    while (datas.hasNext){
      listBuffer += datas.next()
    }

    //清空保存的状态
    dataList.clear()
    //使用 scala List的方法进行排序，取前N个
    val top3: ListBuffer[HotItemClik] = listBuffer.sortWith(_.clicCount > _.clicCount).take(3)

    out.collect(
      s"""
         |窗口结束时间：${new Timestamp(timestamp)}
         |---------------
         |Top3商品：${top3.mkString("\n")}
       """.stripMargin

    )

  }
}


//预聚和函数
class MyaggregateFunction  extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
//全窗口函数，输入类型是 预聚和函数的输出
class MyProcessWindowFunction extends ProcessWindowFunction[Long,HotItemClik, Long, TimeWindow]{
  // 一次进入这个方法里的，只能是一个组的数据，否则，参数中的key无法确定
  //传入全窗口函数的数据，是统计的结果，每个商品只有一条，而且每个商品都是一个分组，所以这里的element中只有一条数据，就是该商品的统计结果
  //  key，count
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[HotItemClik]): Unit = {
    //封装样例类，因为只有一条数据所以不需要遍历element，next去第一条就行；
    //context.window.getend是为了得到窗口结束时间
    out.collect(HotItemClik(key,elements.iterator.next(),context.window.getEnd))


  }
}

/**
  * topN的样例类
  * @param itemId
  * @param clicCount
  * @param windowEndTime
  */
case class HotItemClik(itemId: Long, clicCount: Long, windowEndTime:Long)
