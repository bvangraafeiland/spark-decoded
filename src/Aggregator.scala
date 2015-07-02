import scala.collection.mutable

/**
 * Created by Bastiaan on 01-07-2015.
 */
class Aggregator[K,V,C] (createCombiner: V => C, mergeValue: (C,V) => C, mergeCombiners: (C,C) => C) {

  def combineValuesByKey(iter: Iterator[(K,V)]): Iterator[(K,C)] = {
    val combiners: mutable.HashMap[K,C] = new mutable.HashMap()

    iter.foreach(pair => {
      val key = pair._1
      val value = pair._2
      if (combiners contains key)
        combiners.update(key, mergeValue(combiners.apply(key), value))
      else
        combiners.update(key, createCombiner(value))
    })

    combiners.toIterator
  }
}
