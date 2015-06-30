/**
 * Created by Bastiaan on 24-06-2015.
 */
class FileRDD(context: Context, path: String) extends RDD[String](context, Nil) {

  override def compute(p: Partition): Iterator[String] = io.Source.fromFile(path).getLines()

  override def partitions: Array[Partition] = ???
}
