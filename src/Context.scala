import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 30-06-2015.
 */
class Context {

  private val nextRddId = new AtomicInteger(0)

  def newRddId(): Int = nextRddId.getAndIncrement()

  def runJob[T,U](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int], resultHandler: (Int, U) => Unit): Unit = {
    partitions.foreach(index => {
      val result = func(rdd.compute(rdd.partitions(index)))

      resultHandler(index, result)
    })
  }

  def runJob[T,U](rdd: RDD[T], func: Iterator[T] => U, resultHandler: (Int, U) => Unit): Unit = runJob[T,U](rdd, func, 0 to rdd.partitions.length, resultHandler)

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U, partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    runJob[T,U](rdd, func, partitions, (index, result) => results(index) = result)
    results
  }

  def runJob[T,U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = runJob[T,U](rdd, func, rdd.partitions.indices)

  def textFile(path: String, numPartitions: Int = 2): RDD[String] = new FileRDD(this, path, numPartitions)
}
