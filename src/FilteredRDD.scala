/**
 * Created by bastiaan on 26-5-15.
 */
class FilteredRDD[T](parent: RDD[T], f: T => Boolean) extends RDD[T] {

  override def count(): Long = collect().length

  override def collect(): Seq[T] = parent.collect().filter(f)
}
