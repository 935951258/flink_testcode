package com.team.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 预聚和函数
  * @tparam T
  */
class SimplePreAggregateFunction[T] extends AggregateFunction[T,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(value: T, accumulator: Long): Long = accumulator +1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
