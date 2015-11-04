import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 25-05-2015.
 */

abstract class RDD[T: ClassTag](val context: SparkContext, deps: Seq[Dependency[_]]) {

  def this(oneParent: RDD[_]) = this(oneParent.context, Seq(new OneToOneDependency(oneParent)))

  private var shouldPersist = false
  private val persistedPartitions = new mutable.HashMap[Int, List[T]]

  // Representation

  val id = context.newRddId()

  val partitioner: Option[Partitioner] = None

  private var _partitions: Array[Partition] = null
  protected def getPartitions: Array[Partition]
  final def partitions = {
    if (_partitions == null)
      _partitions = getPartitions
    _partitions
  }

  private var _dependencies: Seq[Dependency[_]] = null
  protected def getDependencies: Seq[Dependency[_]] = deps
  final def dependencies = {
    if (_dependencies != null)
      _dependencies = getDependencies
    _dependencies
  }

  protected def compute(p: Partition): Iterator[T]

  // distributed functionality
//  def preferredLocations(p: Partition): Seq[String] = ???


  // Transformations

  def map[U: ClassTag](f: T => U): RDD[U] = new MappedRDD[T,U](this, _.map(f))

  def mapPartitions[U: ClassTag](f: Iterator[T] => Iterator[U], preservePartitioner: Boolean = false): RDD[U] = new MappedRDD[T,U](this, f, preservePartitioner)

  def flatMap[U: ClassTag](f: T => Seq[U]) : RDD[U] = new MappedRDD[T,U](this, _.flatMap(f))

  def filter(f: T => Boolean): RDD[T] = new MappedRDD[T,T](this, _.filter(f), preservePartitioner = true)

  def union(other: RDD[T]): RDD[T] = new UnionRDD(context, Seq(this, other))

  // Actions

  def count(): Long = context.runJob(this, (iter: Iterator[T]) => iter.size).sum

  def collect(): Array[T] = {
    val results = context.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  def reduce(f: (T,T) => T): T = {
    var result: Option[T] = None

    val reducePartition = (iter: Iterator[T]) =>
      // Guard against empty iterators
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

    result.getOrElse(throw new UnsupportedOperationException("empty dataset"))
  }

  def first(): T = take(1).head

  def take(n: Int): Seq[T] = if (n == 0) Seq[T]() else {
    //TODO move to runJob functions
    val numPartitions = partitions.length

    def takeFromPartitions(amount: Int, numPartitions: Int, currentPartitionIndex: Int, collected: Seq[T]): Seq[T] =
      if (currentPartitionIndex == numPartitions)
        collected

      else {
        val mergedResult = collected ++ iterator(partitions(currentPartitionIndex)).take(amount)
        if (mergedResult.size == collected.size + amount)
          mergedResult
        else
          takeFromPartitions(amount - (mergedResult.size - collected.size), numPartitions, currentPartitionIndex + 1, mergedResult)
      }

    takeFromPartitions(n, numPartitions, 0, Seq())
  }

  final def iterator(partition: Partition): Iterator[T] = {
    if (shouldPersist)
      persistedPartitions.getOrElseUpdate(partition.index, compute(partition).toList).toIterator
    else {
      println("Computing partition " + partition.index + " of RDD " + id)
      compute(partition)
    }
  }

  def persist(): this.type = {
    shouldPersist = true
    this
  }
}

object RDD {
  implicit def rddToPairRdd[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)]): PairRDD[K,V] = new PairRDD(rdd)

  implicit def rddToOrderedRdd[K: Ordering: ClassTag,V](rdd: RDD[(K,V)]): OrderedRDD[K,V] = new OrderedRDD(rdd)
}