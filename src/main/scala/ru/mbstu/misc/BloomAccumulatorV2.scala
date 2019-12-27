package ru.mbstu.misc

import breeze.util.BloomFilter
import org.apache.spark.util.AccumulatorV2

class BloomAccumulatorV2(private var bf: BloomFilter[String]) extends AccumulatorV2[String, BloomFilter[String]] {
  private var isEmpty = true

  override def reset(): Unit = {
    isEmpty = true
  }

  override def add(v: String): Unit = {
    bf += v
    isEmpty = false
  }

  override def isZero: Boolean = {
    isEmpty
  }

  override def copy(): AccumulatorV2[String, BloomFilter[String]] = {
    new BloomAccumulatorV2(bf)
  }

  override def merge(other: AccumulatorV2[String, BloomFilter[String]]): Unit = {
    bf = bf | other.value
  }

  override def value: BloomFilter[String] = {
    bf
  }
}