/**
 * Created by Bastiaan on 28-06-2015.
 */
abstract class Dependency[T] {
  def rdd: RDD[T]
}

abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}

class RangeDependency[T](rdd: RDD[T], parentStartIndex: Int, childStartIndex: Int, length: Int) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] =
    if (partitionIsWithinRange(partitionId))
      List(getParentPartitionId(partitionId))
    else
      Nil

  private def getParentPartitionId(partitionId: Int): Int = partitionId - childStartIndex + parentStartIndex
  private def partitionIsWithinRange(partitionId: Int): Boolean = partitionId >= childStartIndex && partitionId < childStartIndex + length
}

class ShuffleDependency[K,V,C](
    _rdd: RDD[(K,V)],
    val partitioner: Partitioner,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None)
  extends Dependency[(K,V)] {

  override def rdd: RDD[(K, V)] = _rdd
}