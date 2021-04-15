package com.team.apiTest

import java.io.{FileInputStream, InputStream}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}

object TransfromTest {

  def main(args: Array[String]): Unit = {

    //创建执行对象
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    //从文件中读取数据
    val inputDataDtream: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\tt.txt")

    //1.基本转换
    val dataStream: DataStream[SensorReading] = inputDataDtream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        //样例类转换
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })


      //2.分组滚动聚合
    val keyedStream: KeyedStream[SensorReading, String] = dataStream
      // .keyBy("id") //输入为样例类类型,keyBy分区之后返回的key的类型为元组类型
      //.keyBy(data => data.id) //传函数
      .keyBy(new MyIDSelector)//自定义函数 key选择器  keyBy之后得到 KeyedStream(分区流，键控流)

    val reduceStream: DataStream[SensorReading] = keyedStream
      //sum后得到datastream
      //.sum(1)
      //.min("temperature")//keyBy分组之后的 min操作都只针对自己分组内的 temperature字段比较，且其他字段为第一次出现的字段
      // .minBy("temperature")//其他字段为 相对应的值
      .reduce(new Myreduce
      // (curRes, newData) =>
      //取当前分区内最大时间戳和最小温度值
      //SensorReading(curRes.id,curRes.timestamp.max(newData.timestamp),curRes.temperature.min(newData.temperature))
    )

    //3.分流操作
      //得到splitStream
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30)
        Seq("high")
      else
        Seq("low")
    })
    //获取高温流 和低温流 和所有流
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high","low")

   /*   highTempStream.print("high")
      lowTempStream.print("low")
      allTempStream.print("all")*/

    //4.合流操作

    //生成报警流
    val warningStream: DataStream[(String, Double)] = highTempStream.map(
      //转换加上报警信息
      data => (data.id, data.temperature)
    )

    //将报警流和低温流合并返回ConnectedStreams
    //生成合并流
    val connectStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    //分别对两条流进行map操作
    // 生成DataStream
     val resultStream: DataStream[Object] =connectStream.map(
       (warningData: (String, Double)) =>(warningData._1,warningData._2,"hige temp warning"),
       (lowTempData: SensorReading) =>(lowTempData.id,"normal")
    )



    resultStream.print()
    //dataStream.print()
    environment.execute("job")

  }
}

//自定义函数，Key选择器
class MyIDSelector() extends KeySelector[SensorReading,String] {
  override def getKey(in: SensorReading): String = in.id
}

class Myreduce()extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t.timestamp.max(t1.timestamp),t.temperature.min(t1.temperature))

  }
}
