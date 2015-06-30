/**
 * Created by Bastiaan on 25-05-2015.
 */

abstract class RDD[T](context: Context, deps: Seq[Dependency[_]]) {

  def this(oneParent: RDD[_]) = this(Seq(new OneToOneDependency(oneParent)))

  // Representation

  def partitions: Array[Partition]

  def dependencies: Seq[Dependency[_]] = deps

//  def iterator(p: Partition, parentIters: Seq[Iterator]): Iterator[T] = compute(p)

  def compute(p: Partition): Iterator[T]

  def preferredLocations(p: Partition): Seq[String] = ???


  // Transformations

  def map[U](f: T => U): RDD[U] = new MappedRDD[T,U](this, iter => iter.map(f))

  def flatMap[U](f: T => Seq[U]) : RDD[U] = new MappedRDD[T,U](this, iter => iter.flatMap(f))

  def filter(f: T => Boolean): RDD[T] = new MappedRDD[T,T](this, iter => iter.filter(f))

  def union(other: RDD[T]): RDD[T] = new UnionRDD(Seq(this, other))

  // Actions

  def count(): Long = context.runJob(this, (iter: Iterator[T]) => iter.size).sum

  def collect(): Seq[T] = {
    val results = context.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  def reduce(f: (T,T) => T): T = {
    var result: Option[T] = None

    val reducePartition = (iter: Iterator[T]) =>
      if (iter.hasNext) Some(iter.reduce(f))
      else None

    val mergeResults = (index: Int, reducedPartition: Option[T]) => {
      if (reducedPartition.isDefined)
        result = result match {
          case Some(value) => Some(f(reducedPartition.get, value))
          case None => reducedPartition
        }
    }

    context.runJob(this, reducePartition, mergeResults)

    result.get
  }

  def first(): T = take(1).head

  def take(n: Int): Seq[T] = if (n == 0) Seq[T]() else {
    val numPartitions = partitions.length

    def takeFromPartitions(amount: Int, numPartitions: Int, currentPartitionIndex: Int, collected: Seq[T]): Seq[T] =
      if (currentPartitionIndex == numPartitions)
        collected

      else {
        val mergedResult = collected ++ compute(partitions(currentPartitionIndex)).take(amount)
        if (mergedResult.size == collected.size + amount)
          mergedResult
        else
          takeFromPartitions(amount - (mergedResult.size - collected.size), numPartitions, currentPartitionIndex + 1, mergedResult)
      }

    takeFromPartitions(n, numPartitions, 0, Seq())
  }
}

object RDD {
  implicit def pairFunctions[K,V](rdd: RDD[(K,V)]): PairRDD[K,V] = {
    new PairRDD(rdd)
  }
}