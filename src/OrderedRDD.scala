/**
 * Created by Bastiaan on 08-07-2015.
 */
class OrderedRDD[K: Ordering, V](rdd: RDD[(K,V)]) {

  private val ordering = implicitly[Ordering[K]]

  def sortByKey(ascending: Boolean = true, numPartitions: Int = rdd.partitions.length): RDD[(K,V)] = {

  }
}
