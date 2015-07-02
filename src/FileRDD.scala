/**
 * Created by Bastiaan on 24-06-2015.
 */
class FileRDD(context: Context, path: String, numPartitions: Int) extends RDD[String](context, Nil) {

  override def compute(p: Partition): Iterator[String] = io.Source.fromFile(path).getLines()

  override def partitions: Array[Partition] = Array(new FilePartition(id, 0)) //Array.tabulate(numPartitions)(i => new FilePartition(id, i))
}
