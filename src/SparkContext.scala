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
    val computePartitions = Future.sequence(partitions.map(partitionIndex => {
      Future {
        if (nodeFailed(rdd, partitionIndex))
          throw new NodeFailureException(rdd, partitionIndex)

        func(rdd.iterator(rdd.partitions(partitionIndex)))
      } map {
        result => resultHandler(partitionIndex, result)
      } recover {
        case e: NodeFailureException[T] =>
          println(e.getMessage)
          runJob(rdd, func, List(e.partitionId), resultHandler)
      }
    }))

    Await.ready(computePartitions, Duration.Inf)
  }

  def nodeFailed[T](rdd: RDD[T], partitionIndex: Int): Boolean = Random.nextInt(100) < 15

  def runJob[T,U](rdd: RDD[T], func: Iterator[T] => U, resultHandler: (Int, U) => Unit): Unit = runJob[T,U](rdd, func, rdd.partitions.indices, resultHandler)

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T, U](rdd, func, partitions, (index, result) => results(index) = result)
    results
  }

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = runJob[T,U](rdd, func, rdd.partitions.indices)

  def textFile(path: String, numPartitions: Int = SparkContext.defaultNumPartitions): RDD[String] = new FileRDD(this, path, numPartitions)

  def parallelize[T: ClassTag](seq: Seq[T], numPartitions: Int = SparkContext.defaultNumPartitions): RDD[T] = new ParallelCollectionRDD(this, seq, numPartitions)
}

object SparkContext {
  val defaultNumPartitions = Runtime.getRuntime.availableProcessors
}