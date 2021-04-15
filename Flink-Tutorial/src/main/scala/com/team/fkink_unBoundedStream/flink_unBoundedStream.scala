package com.team.fkink_unBoundedStream

import org.apache.flink.streaming.api.scala._

object flink_unBoundedStream {

  def main(args: Array[String]): Unit = {

    //创建执行对象
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据
    val sockerDS: DataStream[String] = environment.socketTextStream("hadoop701",9999)

    //处理数据
    sockerDS.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()

    //启动
    environment.execute()
  }


}
