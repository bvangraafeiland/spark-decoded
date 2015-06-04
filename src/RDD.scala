/**
 * Created by Bastiaan on 25-05-2015.
 */
abstract class RDD[T] {

  // Transformations

  def map[U](f: T => U): RDD[U] = new MappedRDD(this, f)

  def flatMap[U](f: T => Seq[U]) : RDD[U] = ???

  def filter(f: T => Boolean): RDD[T] = new FilteredRDD(this, f)

  def union(other: RDD[T]): RDD[T] = new UnionRDD(Seq(this, other))

  def intersection(other: RDD[T]) : RDD[T] = ???



  // Actions

  def count(): Long

  def collect(): Seq[T]

  def reduce(f: (T,T) => T): T = collect().reduce(f)

  def first(): T = collect().head

  def take(n: Int): Seq[T] = collect().take(n)
}