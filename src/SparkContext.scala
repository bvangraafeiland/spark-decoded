import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Random}
import scala.concurrent.duration._

/**
 * Created by Bastiaan on 30-06-2015.
 */
class SparkContext {

  private val nextRddId = new AtomicInteger(0)

  def newRddId(): Int = nextRddId.getAndIncrement()

  def runJob[T,U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int], resultHandler: (Int, U) => Unit): Unit = {
//    var failedPartitions = List[Int]()

    val computePartitions = Future.sequence(partitions.map(partitionIndex => {
      val future = Future {
        if (nodeFailed(rdd, partitionIndex))
          throw new NodeFailureException(rdd, partitionIndex)

        func(rdd.iterator(rdd.partitions(partitionIndex)))
      }
      future.onComplete {
        case Success(result) => resultHandler(partitionIndex, result)
        case Failure(e: NodeFailureException[T]) =>
          e.printStackTrace()
//          failedPartitions +:= e.partitionId
      }

      future
    }))

    Await.ready(computePartitions, Duration.Inf)
  }

  def nodeFailed(rdd: RDD[_], partitionIndex: Int): Boolean = Random.nextInt(100) < 90

  def runJob[T,U](rdd: RDD[T], func: Iterator[T] => U, resultHandler: (Int, U) => Unit): Unit = runJob[T,U](rdd, func, rdd.partitions.indices, resultHandler)

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)

    runJob[T, U](rdd, func, partitions, (index, result) => results(index) = result)
//    var failedPartitions = Seq[Int]()
//    do {
//      failedPartitions = runJob[T, U](rdd, func, partitions, (index, result) => results(index) = result)
//    } while (failedPartitions.nonEmpty)

    results
  }

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = runJob[T,U](rdd, func, rdd.partitions.indices)

  def textFile(path: String, numPartitions: Int = SparkContext.defaultNumPartitions): RDD[String] = new FileRDD(this, path, numPartitions)

  def parallelize[T: ClassTag](seq: Seq[T], numPartitions: Int = SparkContext.defaultNumPartitions): RDD[T] = new ParallelCollectionRDD(this, seq, numPartitions)
}

object SparkContext {
  val defaultNumPartitions = 2
}