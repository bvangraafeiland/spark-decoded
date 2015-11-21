import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 01-07-2015.
 */
abstract class Partitioner(partitionCount: Int) {

  def getPartition(key: Any): Int

  def numPartitions: Int = partitionCount
}

object Partitioner {
  val defaultPartitioner = new HashPartitioner(SparkContext.defaultNumPartitions)
}

class HashPartitioner(partitionCount: Int) extends Partitioner(partitionCount) {

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, partitionCount)
  }

  override def equals(obj: scala.Any): Boolean = obj match {
    case h: HashPartitioner => h.numPartitions == partitionCount
    case _ => false
  }

  override def hashCode(): Int = partitionCount

  /* Calculates 'x' modulo 'mod', takes to consideration sign of x,
  * i.e. if 'x' is negative, than 'x' % 'mod' is negative too
  * so function return (x % mod) + mod in that case.
  */
  private def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}

/**
 * Partitions key-value records in roughly same sized partitions.
 *
 * Note: takes the entire contents of the RDD, rather than just a sample.
 */
class RangePartitioner[K: Ordering: ClassTag, V](partitionCount: Int, rdd: RDD[(K,V)], val ascending: Boolean = true) extends Partitioner(partitionCount) {

  private val rangeBounds: Array[K] = {
    if (partitionCount <= 1)
      Array.empty
    else {
      val (numItems, sketched) = sketch(rdd.map(_._1))
      if (numItems == 0)
        Array.empty
      else {
        val candidates = ArrayBuffer.empty[(K,Float)]
        sketched.foreach { case (n, keys) =>
          val weight: Float = 1 / n
          for (key <- keys)
            candidates += ((key, weight))
        }
        determineBounds(candidates, partitionCount)
      }
    }
  }

  /**
   * Sketches the input RDD
   *
   * @param rdd the input RDD to sketch
   * @return (total number of items in the RDD, array of (number of items in partition, items in partition))
   */
  private def sketch(rdd: RDD[K]) = {
    val sketched = rdd.mapPartitions(iter => {
      val (content, n) = iteratorContentsAndSize(iter)
      Iterator((n, content))
    }).collect()
    val numItems = sketched.map(_._1).sum
    (numItems, sketched)
  }

  private def iteratorContentsAndSize[T: ClassTag](iterator: Iterator[T]): (Array[T], Int) = {
    var count = 0
    var content = Seq[T]()
    while (iterator.hasNext) {
      content = iterator.next() +: content
      count += 1
    }
    (content.toArray, count)
  }

  private def determineBounds(candidates: ArrayBuffer[(K, Float)], partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < ordered.size) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight > target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    val ordering = implicitly(Ordering[K])

    // Naive search
    while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
      partition += 1
    }

    partition
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }
}