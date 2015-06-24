/**
 * Created by Bastiaan on 26-05-2015.
 */
class MappedRDD[T,U](parent: RDD[T], f: Iterator[T] => Iterator[U]) extends RDD[U] {

  override def count(): Long = parent.count()

  override def collect(): Seq[U] = parent.collect().map(f)
}
