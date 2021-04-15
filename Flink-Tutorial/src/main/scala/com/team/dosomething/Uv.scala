package com.team.dosomething

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object Uv {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    environment.setParallelism(1)
    //设置时间语义
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.读取文件
    val Datas: DataStream[String] = environment.readTextFile("Data/UserBehavior.csv")
    //2.转换成样例类
    val userBehaviorDS: DataStream[UserBehavior] = Datas.map(line => {
      val stringsArray: Array[String] = line.split(",")

      UserBehavior(
        stringsArray(0).toLong,
        stringsArray(1).toLong,
        stringsArray(2) toInt,
        stringsArray(3),
        stringsArray(4).toLong
      )
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //过滤出pv
    val filterDS: DataStream[UserBehavior] = userBehaviorDS.filter(_.behavior == "pv")
    //转换格式
    val userDS: DataStream[(String, Long)] = filterDS.map(data=>("uv",data.userId))
    //按照uv 分组
    val userKS: KeyedStream[(String, Long), String] = userDS.keyBy(_._1)
    //按照分组统计uv值
    val uvDS: DataStream[Long] = userKS
      .timeWindow(Time.hours(1))
      .process(
        //全窗口函数
        new ProcessWindowFunction[(String, Long), Long, String, TimeWindow] {
          //定义一个set去重
          val uvCount: mutable.Set[Long] = mutable.Set[Long]()

          override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[Long]): Unit = {

            //1.遍历数据将,将userid存入Set中
            for (elem <- elements) {
              uvCount.add(elem._2)
            }
            //2.统计Set长度（元素个数，即uv值），并通过采集器往下游发送
            out.collect(uvCount.size)
            //清空本次窗口的 id 次数
            uvCount.clear()
          }
        }
      )

    uvDS.print("uv")

    environment.execute("uv")
  }
}
