/**
 * Created by bastiaan on 27-5-15.
 */
class UnionRDD[T](rdds: Seq[RDD[T]]) extends RDD[T] {

  override def count(): Long = rdds.reduce((rdd1, rdd2) => rdd1.count() + rdd2.count())

  override def collect(): Seq[T] = rdds.reduce((rdd1, rdd2) => rdd1.collect() ++ rdd2.collect())
}
