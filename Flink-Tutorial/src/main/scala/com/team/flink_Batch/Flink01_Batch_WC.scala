package com.team.flink_Batch

import org.apache.flink.api.scala._

object Flink01_Batch_WC {

  def main(args: Array[String]): Unit = {

    //初始化
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val words: DataSet[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\hello.txt")

    //转换
    val WC: AggregateDataSet[(String, Int)] = words.flatMap(
      line => line.split(" ")
    ).map((_, 1))
      .groupBy(0)
      .sum(1)
    WC.print()

  }
}
