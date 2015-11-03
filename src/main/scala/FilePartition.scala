/**
 * Created by Bastiaan on 01-07-2015.
 */
class FilePartition(rddId: Int, partId: Int) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = partId

  // A better default implementation of HashCode
  override def hashCode(): Int = 41 * (41 + rddId) + partId
}
