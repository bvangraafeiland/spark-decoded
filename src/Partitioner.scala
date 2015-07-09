/**
 * Created by Bastiaan on 01-07-2015.
 */
abstract class Partitioner {

  def getPartition(key: Any): Int

  def numPartitions: Int
}

object Partitioner {

  val defaultPartitioner = new HashPartitioner(Context.defaultNumPartitions)
}

class HashPartitioner(partitionCount: Int) extends Partitioner {

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, partitionCount)
  }

  override def numPartitions: Int = partitionCount

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
class RangePartitioner[K: Ordering, V](rdd: RDD[(K,V)], partitionCount: Int, val ascending: Boolean = true) extends Partitioner {

  private val rangeBounds: Array[K] = {
    if (partitionCount <= 1)
      Array.empty
    else {
      val (numItems, sketched) = sketch(rdd.map(_._1))
      if (numItems == 0)
        Array.empty
      else {

      }
    }
  }

  private def sketch(rdd: RDD[K]) = {
    val sketched = rdd.mapPartitions(iter => {
      val (content, n) = iteratorContentsAndSize(iter)
      Iterator((n, content))
    }).collect()
    val numItems = sketched.map(_._1).sum
    (numItems, sketched)
  }

  private def iteratorContentsAndSize[T](iterator: Iterator[T]): (Array[T], Int) = {
    var count = 0
    var content = Seq[T]()
    while (iterator.hasNext) {
      content = iterator.next() +: content
      count += 1
    }
    (content.toArray, count)
  }

  override def getPartition(key: Any): Int = ???

  override def numPartitions: Int = partitionCount

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