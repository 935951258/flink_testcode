package com.team.flinkTableAPI

import com.team.apiTest.WaterSensor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object TableAPI {
  def main(args: Array[String]): Unit = {


    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    val datas: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\data.txt")

    val dataDS: DataStream[WaterSensor] = datas.map(line => {
      val strings: Array[String] = line.split(",")
      WaterSensor(
        strings(0),
        strings(1).toLong,
        strings(2).toDouble
      )
    })

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    //基于数据流，转换成一张表，然后进行操作
    val dataTable: Table = tableEnv.fromDataStream(dataDS)

    //tableEnv.createTemporaryView("water",dataTable)

    //查询字段过滤
    val resultTable: Table = dataTable
      .select("id,timestamp")
      .filter("id =='\"ws_001\"'")


    //直接写sql得到转换结果
    val resultSqlTable: Table = tableEnv
      .sqlQuery("select id,timestamp,vc from " + dataTable + "where id = '\"ws_001\"'")
      //.sqlQuery("select * from water where id = '\"ws_001\"'")


    //转换为流数据
    val resultDS: DataStream[(String, Long,Double)] = resultSqlTable.toAppendStream[(String,Long,Double)]


    //resultTable.printSchema()

    resultDS.print()





    environment.execute()

  }

}
