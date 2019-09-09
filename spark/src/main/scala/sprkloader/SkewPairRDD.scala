package sprkloader

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag

object SkewPairRDD {
  
  implicit class SkewPairRDDFunctions[K: ClassTag, V: ClassTag](lrdd: RDD[(K,V)]) extends Serializable {

    val reducers = Config.minPartitions 

    def heavyKeys(): Set[K] = {
      lrdd.mapPartitions{ it => 
        Util.countDistinct(it).filter(_._2 > 1000).iterator }
      .reduceByKey(_ + _)
      .filter(_._2 >= reducers)
      .keys.collect.toSet
    }

    def balanceLeft[S](rrdd: RDD[(K, S)], hkeys: Broadcast[Set[K]]): (RDD[((K, Int), V)], RDD[((K, Int), S)]) = {
      val lrekey = lrdd.mapPartitions{ it =>
        it.zipWithIndex.map{ case ((k,v), i) => 
          (k, { if (hkeys.value(k)) i % reducers else 0 }) -> v
        }
      }
      val rdupp = rrdd.flatMap{ case (k,v) =>
        Range(0, {if (hkeys.value(k)) reducers else 1 }).map(id => (k, id) -> v) 
      }
      (lrekey, rdupp)
    }

    def joinSkewLeft[S](rrdd: RDD[(K, S)]): RDD[(K, (V, S))] = { 
      val hk = heavyKeys
      if (hk.nonEmpty) {
        val hkeys = lrdd.sparkContext.broadcast(hk)
        val (rekey,dupp) = lrdd.balanceLeft(rrdd, hkeys)
        rekey.join(dupp).map{ case ((k, _), v) => k -> v }
      }
      else lrdd.join(rrdd) 
    }

    def groupByLabel[R: ClassTag,S: ClassTag](f: (K,V) => (R,S)): RDD[(R, Iterable[S])] = {
      val groupBy = (i: Iterator[(K,V)]) => {
        val hm = HashMap[R, Vector[S]]()
        i.foreach{ v0 =>
          val (k,v) = f(v0._1, v0._2) 
          hm(k) = hm.getOrElse(k, Vector()) :+ v
        }
        hm.iterator
      }
      lrdd.mapPartitions(groupBy)
    }

  }

}
