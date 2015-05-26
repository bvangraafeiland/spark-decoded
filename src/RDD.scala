/**
 * Created by Bastiaan on 25-05-2015.
 */
abstract class RDD[T] {

  // Transformations

  def map[U](f: T => U): RDD[U] = new MappedRDD(this, f)

  def filter(f: T => Boolean): RDD[T]

  def union(other:RDD[T]): RDD[T]


  // Actions

  def count(): Long

  def collect(): Seq[T]

  def reduce(f: (T,T) => T): T
}