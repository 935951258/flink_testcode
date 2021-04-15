package com.team


import org.apache.flink.api.scala._
object wordcount {


  def main(args: Array[String]): Unit = {

    //创建一个批处理执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  environment.setParallelism(1)
    //从文件中读取数据
    val inputPath = "D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = environment.readTextFile(inputPath)

    //分词之后做count
    val wordcountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    //打印
        wordcountDataSet.print()
    //environment.execute("ss")
  }

}
