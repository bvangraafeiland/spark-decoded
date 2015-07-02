import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 26-05-2015.
 */
class MappedRDD[T,U: ClassTag](parent: RDD[T], f: Iterator[T] => Iterator[U]) extends RDD[U](parent) {

  override def partitions: Array[Partition] = parent.partitions

  override def compute(p: Partition): Iterator[U] = f(parent.compute(p))
}
