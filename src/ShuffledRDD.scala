/**
 * Created by Bastiaan on 01-07-2015.
 */
class ShuffledRDD[K,V,C](parent: RDD[(K,V)], part: Partitioner, aggregator: Option[Aggregator[K,V,C]] = None) extends RDD[(K,C)](parent.context, Nil) {

  override def partitions: Array[Partition] = Array.tabulate(part.numPartitions)(i => new ShuffledRDDPartition(i))

  override def compute(p: Partition): Iterator[(K,C)] = {
    val theIterator = parent.partitions.map(p => parent.compute(p)).reduce(_ ++ _).filter(el => part.getPartition(el._1) == p.index)
    aggregator match {
      case Some(aggr) => aggr.combineValuesByKey(theIterator)
      case None => theIterator.asInstanceOf[Iterator[(K,C)]]
    }
  }

  override val partitioner: Option[Partitioner] = Some(part)

  override def dependencies: Seq[Dependency[_]] = List(new ShuffleDependency(parent, part))
}

class ShuffledRDDPartition(ind: Int) extends Partition {
  /**
   * Get the partition's index within its parent RDD
   */
  override def index: Int = ind
}