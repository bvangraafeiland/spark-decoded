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
class RangePartitioner[K: Ordering, V](rdd: RDD[(K,V)], partitionCount: Int) extends Partitioner {

  private val rangeBounds = {
    if (partitionCount <= 1)
      Array.empty
    else {

    }
  }

  override def getPartition(key: Any): Int = ???

  override def numPartitions: Int = partitionCount
}