/**
 * Created by Bastiaan on 25-05-2015.
 */
abstract class RDD[T] {

  // Representation

  def partitions(): Seq[Partition] = ???

  def dependencies(): Seq[RDD] = ???

  def iterator(p: Partition, parentIters: Seq[Iterator]): Iterator[T] = compute(p)

  def compute(p: Partition): Iterator[T]

  def preferredLocations(p: Partition): Seq[String] = ???


  // Transformations

  def map[U](f: T => U): RDD[U] = new MappedRDD(this, iter => iter.map(f))

  def flatMap[U](f: T => Seq[U]) : RDD[U] = new MappedRDD(this, iter => iter.flatMap(f))

  def filter(f: T => Boolean): RDD[T] = new MappedRDD(this, iter => iter.filter(f))

  def union(other: RDD[T]): RDD[T] = new UnionRDD(Seq(this, other))


  // Actions

  def count(): Long

  def collect(): Seq[T] = partitions().map(p => compute(p)).reduce((s1, s2) => s1.toSeq ++ s2.toSeq)

  def reduce(f: (T,T) => T): T = collect().reduce(f)

  def first(): T = take(1).head

  def take(n: Int): Seq[T] = collect().take(n)
}