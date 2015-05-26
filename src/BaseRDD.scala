/**
 * Created by Bastiaan on 26-05-2015.
 */
class BaseRDD[T](seq: Seq[T]) extends RDD[T] {

  override def count(): Long = seq.length

  override def collect(): Seq[T] = seq
}
