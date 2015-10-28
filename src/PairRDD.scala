import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 28-06-2015.
 */
class PairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]) {

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

  def reduceByKey(f: (V,V) => V, numPartitions: Int): RDD[(K,V)] = combineByKey(identity, f, f, new HashPartitioner(numPartitions))

  def reduceByKey(f: (V,V) => V): RDD[(K,V)] = combineByKey(identity, f, f, Partitioner.defaultPartitioner)

  def groupByKey(numPartitions: Int): RDD[(K,Seq[V])] = combineByKey(v => Seq(v), (seq, v) => v +: seq, (seq1, seq2) => seq1 ++ seq2, new HashPartitioner(numPartitions))

  def groupByKey(): RDD[(K,Seq[V])] = combineByKey(v => Seq(v), (seq, v) => v +: seq, (seq1, seq2) => seq1 ++ seq2, Partitioner.defaultPartitioner)

  def partitionBy(partitioner: Partitioner): RDD[(K,V)] =
    if (rdd.partitioner contains partitioner)
      rdd
    else
      new ShuffledRDD[K,V,V](rdd, partitioner)

  def mapValues[U](f: V => U): RDD[(K,U)] = new MappedRDD[(K,V),(K,U)](rdd, _.map(tuple => (tuple._1, f(tuple._2))), true)

  def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U, comboOp: (U, U) => U): RDD[(K,U)] = combineByKey(v => seqOp(zeroValue, v), seqOp, comboOp, partitioner)

  def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, comboOp: (U, U) => U): RDD[(K,U)] = aggregateByKey(zeroValue, Partitioner.defaultPartitioner)(seqOp, comboOp)

  // actions

  def countByKey(): Map[K, Long] = rdd.mapValues(_ => 1L).reduceByKey(_ + _).collect().toMap

  /**
   * Return the list of values in the RDD for key `key`. This operation is done efficiently if the
   * RDD has a known partitioner by only searching the partition that the key maps to.
   */
  def lookup(key: K): Seq[V] =
    rdd.partitioner match {
      case Some(p) =>
        val index = p.getPartition(key)
        val process = (it: Iterator[(K, V)]) => {
          val buf = new ArrayBuffer[V]
          for (pair <- it if pair._1 == key) {
            buf += pair._2
          }
          buf
        } : Seq[V]
        val res = rdd.context.runJob(rdd, process, Array(index))
        res(0)
      case None =>
        rdd.filter(_._1 == key).map(_._2).collect()
    }
}
