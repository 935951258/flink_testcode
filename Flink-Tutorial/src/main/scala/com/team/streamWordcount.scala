package com.team

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object streamWordcount {

  def main(args: Array[String]): Unit = {

    val parameter: ParameterTool = ParameterTool.fromArgs(args)

    val host :String = parameter.get("host")
    val port: Int = parameter.getInt("port")

    //流处理wordcount

    //创建流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment



    //接受socket数据流
    val textDataStream = environment.socketTextStream(host,port)

    //逐一读取数据，打散进行wordcount   正则匹配空格
    val streamWordCountDataSet = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).slotSharingGroup("2")//slot 共享组，如果配置了那么就不能和不同组的任务合并，默认组为default
      .map( (_,1) ).disableChaining()//拒绝合并任务  算子链
      .keyBy(0)
      .sum(1).startNewChain()//跟前面的任务断开，后面的可以合并

    //打印输出
    streamWordCountDataSet.print()//.setParallelism(2) 修改并行度

    //执行任务
    environment.execute("streamWordCountName")



  }

}
