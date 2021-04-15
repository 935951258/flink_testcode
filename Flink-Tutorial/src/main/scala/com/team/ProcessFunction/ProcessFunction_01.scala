package com.team.ProcessFunction

import java.sql.Timestamp

import com.team.apiTest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunction_01 {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    val inputStream: DataStream[String] = environment.socketTextStream("localhost",9999)

    val dateStream: DataStream[SensorReading] = inputStream
      .map(data => {

      val splitArray: Array[String] = data.split(",")

      SensorReading(splitArray(0), splitArray(1).toLong, splitArray(2).toDouble)
    })

    val warmStream: DataStream[String] =dateStream
      .keyBy("id")
      .process( new TempIncreWarning(10000L))

    warmStream.print()
    environment.execute("peocess function job")
  }



}

//自定义 KeyedProcessFunction
case class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{

  //由于需要跟之前的温度做对比，所以需要将上一个温度保存成状态
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",classOf[Double]) )

  //为了删除定时器，还需要保持定时器的事件戳
  lazy val curTimeTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-time",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {

    //取出状态
    val lastTemp: Double = lastTempState.value()
    val curTimers: Long = curTimeTsState.value()

    //将上次温度值的转台更新为当前的温度值
    lastTempState.update(value.temperature)

    //判断当前温度值，如果比之前温度高，并且没有定时器的话，注册10后的定时器
    if (value.temperature > lastTemp && curTimers == 0){
      val ts: Long =ctx.timerService().currentProcessingTime() + interval
      println("当前时间："+ new Timestamp(ctx.timerService().currentProcessingTime()))
      println("定时器触发时间"+new Timestamp(ts))
      //注册定时器
      ctx.timerService().registerProcessingTimeTimer(ts)
      //更新定时器的状态
      curTimeTsState.update(ts)
    }

    //如果温度下降
    else if(value.temperature < lastTemp){
      println("温度下降:"+ (lastTemp - value.temperature)+"度")
      ctx.timerService().deleteProcessingTimeTimer(curTimers)
      curTimeTsState.clear()
    }

  }

  //定时器中的操作
  //如果10秒内温度没有下降，则报警
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //println("温度")
    out.collect("温度值连续"+interval/1000L+"秒上升")

    curTimeTsState.clear()
  }


}