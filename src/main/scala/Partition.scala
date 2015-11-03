/**
 * Created by Bastiaan on 26-05-2015.
 */
trait Partition extends Serializable{
  /**
   * Get the partition's index within its parent RDD
   */
  def index: Int

  // A better default implementation of HashCode
  override def hashCode(): Int = index

  override def equals(obj: scala.Any): Boolean = obj match {
    case p: Partition => p.index == index
    case _ => false
  }
}
