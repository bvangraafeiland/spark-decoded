/**
 * Created by Bastiaan on 24-06-2015.
 */
class FileRDD(context: SparkContext, path: String, numPartitions: Int) extends RDD[String](context, Nil) {

  private lazy val numLines = getFileLines.size

  override def compute(p: Partition): Iterator[String] = {
    val pid = p.index
    val from = (pid * numLines) / numPartitions
    val until = ((pid + 1) * numLines) / numPartitions
    getFileLines.slice(from, until)
  }

  protected def getFileLines: Iterator[String] = io.Source.fromFile(path, "UTF-8").getLines()

  override def getPartitions: Array[Partition] = Array.tabulate(numPartitions)(i => new FilePartition(id, i))
}
