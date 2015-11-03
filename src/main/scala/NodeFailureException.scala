/**
 * Created by Bastiaan on 30-10-2015.
 */
class NodeFailureException[T](val rdd: RDD[T], val partitionId: Int) extends Exception {
  override def getMessage: String = "failed partition " + partitionId + " of RDD " + rdd.id
}
