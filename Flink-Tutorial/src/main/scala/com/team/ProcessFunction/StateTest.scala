package com.team.ProcessFunction

import java.util.concurrent.TimeUnit

import com.team.apiTest.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    //启用检查点，指定触发检查点的间隔时间
    environment.enableCheckpointing(1000L)

    //其他配置
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //一次checkpoint的时间
    environment.getCheckpointConfig.setCheckpointTimeout(30000L)
    //最多同时有两个检查点做操作
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //两次checkpoint之间必须间隔多长时间
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
    //任务挂了之后， true为从默认的checkpoint恢复，false为从自定义的savepoint恢复
    environment.getCheckpointConfig.setPreferCheckpointForRecovery(false)
    //允许失败几次
    environment.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
    //重启策略,重启三次间隔10秒
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L))
    //失败率重启，5分钟内，重启5次 间隔10秒
    environment.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)))
    val stream: DataStream[String] = environment.socketTextStream("localhost",9999)

    val datastream: DataStream[SensorReading] = stream.map((data: String) => {

      val dateArray: Array[String] = data.split(",")

      SensorReading(dateArray(0), dateArray(1).toLong, dateArray(2).toDouble)
    })

    val warningstream: DataStream[(String, Double, Double)] = datastream
      .keyBy("id")
      .map(new TempChangeWarning(10))

    warningstream.print()



    environment.execute("state test job")

  }
}
//自定义mapfunction
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading,(String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //定义状态
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-stamp",classOf[Double]))
  }

  override def map(value: SensorReading): (String, Double, Double) = {

  //从状态中取出上一次的温度值
  val lastTamp: Double = lastTempState.value()

    //更新状态
    lastTempState.update(value.temperature)

  //跟当前温度值计算差值，然后跟阈值比较，如果大于就报警
    //计算差值，取绝对值
    //差值大于10  输出信息，小于10 默认输出
    val diff: Double = (value.temperature - lastTamp).abs

      if (diff > threshold){
        (value.id ,lastTamp ,value.temperature)
      }else{
        (value.id,0.0,0.0)
      }
  }

}