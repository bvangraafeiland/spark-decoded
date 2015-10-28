import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 26-05-2015.
 */
class MappedRDD[T,U: ClassTag](parent: RDD[T], f: Iterator[T] => Iterator[U], preservePartitioner: Boolean = false) extends RDD[U](parent) {

  override def getPartitions: Array[Partition] = parent.partitions

  override def compute(p: Partition): Iterator[U] = f(parent.iterator(p))

  override val partitioner: Option[Partitioner] = if (preservePartitioner) parent.partitioner else None
}
