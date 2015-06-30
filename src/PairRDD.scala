/**
 * Created by Bastiaan on 28-06-2015.
 */
class PairRDD[K,V](rdd: RDD[(K,V)]) {

  def reduceByKey(f: (V,V) => V): RDD[(K,V)] = ???

  def groupByKey(): RDD[(K,V)] = ???
}
