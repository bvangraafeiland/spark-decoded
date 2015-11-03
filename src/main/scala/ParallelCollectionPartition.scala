/**
 * Created by Bastiaan on 04-07-2015.
 */
class ParallelCollectionPartition[T](val rddId: Int, partitionId: Int, values: Seq[T]) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = partitionId

  override def hashCode(): Int = 41 * (41 + rddId) + partitionId

  override def equals(other: Any): Boolean = other match {
    case that: ParallelCollectionPartition[_] =>
      this.rddId == that.rddId && this.partitionId == that.index
    case _ => false
  }

  def iterator: Iterator[T] = values.iterator
}
