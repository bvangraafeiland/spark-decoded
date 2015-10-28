/**
 * Created by Bastiaan on 29-06-2015.
 */
class UnionPartition[T](ind: Int, val parent: RDD[T], parentRDDPartitionIndex: Int) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = ind

  def parentPartition = parent.partitions(parentRDDPartitionIndex)
}
