/**
 * Created by Bastiaan on 01-07-2015.
 */
class Aggregator[K,V,C] (createCombiner: V => C, mergeValue: (C,V) => C, mergeCombiners: C => C) {

}
