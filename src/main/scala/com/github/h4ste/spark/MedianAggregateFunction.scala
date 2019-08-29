package com.github.h4ste.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

class MedianAggregateFunction extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("window_list", ArrayType(DoubleType, containsNull = false)) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new mutable.ArrayBuffer[Double]()
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[mutable.WrappedArray[Double]](0).toBuffer += input.getAs[Double](0)
//    var array = buffer.getAs[mutable.WrappedArray[Double]](0).toBuffer
//    array += input.getAs[Double](0)
//    buffer(0) = array
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[mutable.WrappedArray[Double]](0).toBuffer ++= buffer2.getAs[mutable.WrappedArray[Double]](0)
//    buffer1(0) = buffer1.getAs[mutable.WrappedArray[Double]](0) ++ buffer2.getAs[mutable.WrappedArray[Double]](0)
  }

  override def evaluate(buffer: Row): Any = {
    val seq = buffer.getAs[mutable.WrappedArray[Double]](0)
    MedianUtils.findMedianInPlace(seq)(MedianUtils.midpointPivot): Double
//    val sortedWindow = buffer.getAs[IndexedSeq[Double]](0).sorted
//    val windowSize = sortedWindow.size
//    if (windowSize % 2 == 0) {
//      val index = windowSize / 2
//      (sortedWindow(index) + sortedWindow(index - 1)) / 2
//    } else {
//      var index = (windowSize + 1) / 2 - 1
//      sortedWindow(index)
//    }
  }
}

case class PartitionableSeq(seq: mutable.IndexedSeq[Double], from: Int, until: Int) {
  def apply(n: Int): Double =
    if (from + n < until) seq(from + n)
    else throw new ArrayIndexOutOfBoundsException(n)

  def partitionInPlace(p: Double => Boolean): (PartitionableSeq, PartitionableSeq) = {
    var upper = until - 1
    var lower = from
    while (lower < upper) {
      while (lower < until && p(seq(lower))) lower += 1
      while (upper >= from && !p(seq(upper))) upper -= 1
      if (lower < upper) {
        val tmp = seq(lower); seq(lower) = seq(upper); seq(upper) = tmp
      }
    }
    (copy(until = lower), copy(from = lower))
  }

  def size: Int = until - from

  def isEmpty: Boolean = size <= 0

  override def toString: String = seq mkString("ArraySize(", ", ", ")")
}

object PartitionableSeq {
  def apply(seq: mutable.IndexedSeq[Double]) = new PartitionableSeq(seq, 0, seq.length)
}

object MedianUtils {
  @tailrec def findKMedianInPlace(arr: PartitionableSeq, k: Int)(implicit choosePivot: mutable.IndexedSeq[Double] => Double): Double = {
    val a = choosePivot(arr.seq)
    val (s, b) = arr partitionInPlace (a >)
    if (s.size == k) a
    // The following test is used to avoid infinite repetition
    else if (s.isEmpty) {
      val (s, b) = arr partitionInPlace (a ==)
      if (s.size > k) a
      else findKMedianInPlace(b, k - s.size)
    } else if (s.size < k) findKMedianInPlace(b, k - s.size)
    else findKMedianInPlace(s, k)
  }

  def findMedianInPlace(arr: mutable.IndexedSeq[Double])(implicit choosePivot: mutable.IndexedSeq[Double] => Double): Double =
    findKMedianInPlace(PartitionableSeq(arr), (arr.length - 1) / 2)

  def randomPivot(seq: mutable.IndexedSeq[Double]): Double = seq(Random.nextInt(seq.length))

  def midpointPivot(seq: mutable.IndexedSeq[Double]): Double = seq(seq.length / 2)
}
