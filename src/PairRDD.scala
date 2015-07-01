/**
 * Created by Bastiaan on 28-06-2015.
 */
class PairRDD[K,V](rdd: RDD[(K,V)]) {

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C,
                      partitioner: Partitioner): RDD[(K,C)] = {

  }

  def reduceByKey(f: (V,V) => V): RDD[(K,V)] = ???

  def groupByKey(): RDD[(K,V)] = ???

  def partitionBy(partitioner: Partitioner): RDD[(K,V)] =
    if (rdd.partitioner contains partitioner)
      rdd
    else

}
