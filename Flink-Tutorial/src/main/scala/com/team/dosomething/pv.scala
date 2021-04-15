package com.team.dosomething


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
/**
  * 实时统计每小时内的网站pv
  */
object pv {

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

    //3.处理数据
  val filterDs: DataStream[UserBehavior] = userBehaviorDs.filter(_.behavior == "pv")
    //3.1转换为（pv，1）元组
    val pvAndOneDs: DataStream[(String, Int)] = filterDs.map(pvData=>("pv",1))
    //3.2按照pv进行分组
    val pvAndOneKs: KeyedStream[(String, Int), String] = pvAndOneDs.keyBy(_._1)

    /*val unit: DataStream[(String, Int)] = pvAndOneKs.sum(1)
    unit.print()*/
    //3.3按每小时开窗口
    val pvAndOneWS: WindowedStream[(String, Int), String, TimeWindow] = pvAndOneKs.timeWindow(Time.hours(1))
    //3.4按照分组进行统计求和
    val pvDS: DataStream[(String, Int)] = pvAndOneWS.sum(1)

    //打印
    pvDS.print("pv")

    environment.execute()
  }
}
