import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 08-07-2015.
 */
class OrderedRDD[K: Ordering: ClassTag, V](rdd: RDD[(K,V)]) {

  private val ordering = implicitly[Ordering[K]]

  def sortByKey(ascending: Boolean = true, numPartitions: Int = rdd.partitions.length): RDD[(K,V)] = {
    val part = new RangePartitioner(numPartitions, rdd, ascending)
    new ShuffledRDD[K,V,V](rdd, part).setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }
}
