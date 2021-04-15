package com.team.flink_BoundedStream


import org.apache.flink.streaming.api.scala._

object BoundedStream {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //处理数据
    val wordsStream: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\hello.txt")

    val wc: DataStream[(String, Int)] = wordsStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    wc.print()

    environment.execute()


  }
}
