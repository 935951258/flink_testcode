package com.team.apiTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.util.Random
//输入数据的样例类
case class SensorReading(id:String, timestamp:Long, temperature:Double)

object SourceTest {

  def main(args: Array[String]): Unit = {

    //创建执行环境
     val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
     //environment.setMaxParallelism(1)

    //1.从集合中读取数据
    val stream1: DataStream[SensorReading] = environment.fromCollection( List(
      SensorReading("why", 12345, 22.3),
      SensorReading("so", 12345, 22.3),
      SensorReading("serious", 12345, 22.3),
      SensorReading("a", 12345, 22.3),
      SensorReading("ha", 12345, 22.3),
      SensorReading("?", 12345, 22.3)
    ))

    //从文件读取数据
    val stream2: DataStream[String] = environment.readTextFile("D:\\ideaworkspace\\Flink-Tutorial\\src\\main\\resources\\tt.txt")

    //socket 文本流
    //val value: DataStream[String] = environment.socketTextStream()

    //从kafka 读取数据
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG,"consumer-group")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")

    val stream4: DataStream[String] = environment.addSource( new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties) )


    //自定义Source
     val stream5: DataStream[SensorReading] = environment.addSource(new MySensorSource())




    //打印输出
    stream5.print("stream2").setParallelism(1)
    environment.execute("source test job")
  }

}

//实现自定义的SourceFunction ，自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading]{

  //定义flag, 表示数据源是否正常运行
  var running:Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    //随机生成SensorReading数据
    val random = new Random()
    import java.text.DecimalFormat
    val df = new DecimalFormat("######0.00")
    //随机生成10个传感器的温度值，并且不停的在之前的温度基础上更新（随机上下波动）
    //首先生成10个传感器的初始温度
   var curTemps = 1.to(10).map(
      i =>("sensor_"+i,60 + random.nextGaussian() * 20)

    )

    //无限循环生成随机数
    while (running){
      //在当前温度基础上，随机生成微小波动
      curTemps = curTemps.map(

        data =>(data._1,data._2  + random.nextGaussian() )

      )

      //获取当前系统时间
      val curTs =System.currentTimeMillis()

      //包装成样例类，用ctx发送数据
      curTemps.foreach(
        data => sourceContext.collect(SensorReading(data._1,curTs,data._2))

      )

      Thread.sleep(1000L)
    }

  }


  override def cancel(): Unit = running = false
}