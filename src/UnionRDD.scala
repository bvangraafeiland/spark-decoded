import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by bastiaan on 27-5-15.
 */
class UnionRDD[T: ClassTag](context: SparkContext, rdds: Seq[RDD[T]]) extends RDD[T](context, Nil) {

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdds.map(_.partitions.length).sum)
    var pos = 0
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  override def compute(p: Partition): Iterator[T] = {
    val partition = p.asInstanceOf[UnionPartition[T]]

    rdds(partition.parentIndex).compute(partition.parentPartition)
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps
  }
}
