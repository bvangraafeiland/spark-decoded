/**
 * Created by Bastiaan on 01-07-2015.
 */
abstract class Partitioner {

  def getPartition(key: Any): Int

  def numPartitions: Int
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