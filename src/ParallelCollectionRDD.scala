import scala.reflect.ClassTag

/**
 * Created by Bastiaan on 04-07-2015.
 */
class ParallelCollectionRDD[T: ClassTag](context: SparkContext, data: Seq[T], numPartitions: Int) extends RDD[T](context, Nil) {

  override def getPartitions: Array[Partition] = {
    val dataArray = data.toArray // better performance
    val totalLength = dataArray.length
    (0 until numPartitions).map(index => {
      val start = (index * totalLength) / numPartitions
      val end = ((index + 1) * totalLength) / numPartitions
      new ParallelCollectionPartition(id, index, dataArray.slice(start, end))
    }).toArray
  }

  override def compute(p: Partition): Iterator[T] = p.asInstanceOf[ParallelCollectionPartition[T]].iterator
}
