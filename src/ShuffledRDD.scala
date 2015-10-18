/**
 * Created by Bastiaan on 01-07-2015.
 */
class ShuffledRDD[K,V,C](parent: RDD[(K,V)], part: Partitioner, aggregator: Option[Aggregator[K,V,C]] = None) extends RDD[(K,C)](parent.context, Nil) {

  private var keyOrdering: Option[Ordering[K]] = None
  private val parentData = parent.partitions.map(p => parent.compute(p)).reduce(_ ++ _)

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  override def getPartitions: Array[Partition] = Array.tabulate(part.numPartitions)(i => new ShuffledRDDPartition(i))

  override def compute(p: Partition): Iterator[(K,C)] = {
    val theIterator = parentData.filter(el => part.getPartition(el._1) == p.index)
    val aggregated = aggregator match {
      case Some(aggr) => aggr.combineValuesByKey(theIterator)
      case None => theIterator.asInstanceOf[Iterator[(K,C)]]
    }
    keyOrdering match {
      case Some(order: Ordering[K]) =>
        aggregated.toArray.sortBy(_._1)(order).toIterator
      case None =>
        aggregated
    }
  }

  override val partitioner: Option[Partitioner] = Some(part)

  override def getDependencies: Seq[Dependency[_]] = List(new ShuffleDependency(parent, part))
}

class ShuffledRDDPartition(ind: Int) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = ind
}