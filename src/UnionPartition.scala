/**
 * Created by Bastiaan on 29-06-2015.
 */
class UnionPartition[T](ind: Int, parent: RDD[T], val parentIndex: Int, parentRDDPartitionIndex: Int) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = ind

  def parentPartition = parent.partitions(parentRDDPartitionIndex)
}
