/**
 * Created by bastiaan on 27-5-15.
 */
class UnionRDD[T](rdds: Seq[RDD[T]]) extends RDD[T] {

  override def count(): Long = rdds.map(_.count()).sum

  override def collect(): Seq[T] = rdds.map(_.collect()).reduce((rdd1, rdd2) => rdd1 ++ rdd2)
}
