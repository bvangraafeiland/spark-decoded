/**
 * Created by Bastiaan on 28-06-2015.
 */
class PairRDD[K,V](rdd: RDD[(K,V)]) {

  def combineByKey[C](createCombiner: V => C,
                      mergeValue: (C, V) => C,
                      mergeCombiners: (C, C) => C,
                      partitioner: Partitioner): RDD[(K,C)] = {
    val aggregator = new Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners)

    if (rdd.partitioner contains partitioner) // already a shuffled RDD
      rdd.mapPartitions(iter => aggregator.combineValuesByKey(iter))
    else
      new ShuffledRDD(rdd, partitioner, Some(aggregator))
  }

  def reduceByKey(f: (V,V) => V, numPartitions: Int): RDD[(K,V)] = combineByKey(v => v, f, f, new HashPartitioner(numPartitions))

  def groupByKey(numPartitions: Int): RDD[(K,Seq[V])] = combineByKey(v => Seq(v), (seq, v) => v +: seq, (seq1, seq2) => seq1 ++ seq2, new HashPartitioner(numPartitions))

  def partitionBy(partitioner: Partitioner): RDD[(K,V)] =
    if (rdd.partitioner contains partitioner)
      rdd
    else
      new ShuffledRDD[K,V,V](rdd, partitioner)
}
