package sprkloader

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap
import PairRDDOperations._
import SkewTopRDD._
import Util._

object SkewPairRDD {

  implicit class SkewKeyFunctions[K: ClassTag, V:ClassTag](lrdd: (RDD[(K,V)], RDD[(K,V)], Broadcast[Set[K]])) extends Serializable {

    val light = lrdd._1
    val heavy = lrdd._2
    val heavyKeys = lrdd._3
    val partitions = light.getNumPartitions

    def print: Unit = (light, heavy).print
    def evaluate: Unit = (light, heavy).evaluate

    // non key altering map
    def map[S:ClassTag](f: ((K,V)) => (K, S)): (RDD[(K,S)], RDD[(K,S)], Broadcast[Set[K]]) = {
      val (l, h) = (light, heavy).map(f)
      (l, h, heavyKeys)
    }

    // key altering map
    def map[S:ClassTag](f: ((K,V)) => S): (RDD[S], RDD[S]) = (light, heavy).map(f)

    // filter never alters the bag of heavy keys, if these keys are filtered out
    // a downstream join/nest operation will update the set of heavy keys
    def filter(p: ((K, V)) => Boolean): (RDD[(K,V)], RDD[(K,V)], Broadcast[Set[K]]) = {
      val (l, h) = (light, heavy).filter(p)
      (l, h, heavyKeys)
    }

    // non key altering flatmap
    def flatMap[S:ClassTag](f: ((K,V)) => Vector[(K, S)]): (RDD[(K, S)], RDD[(K, S)], Broadcast[Set[K]]) = {
      val (l, h) = (light, heavy).flatMap(f)
      (l, h, heavyKeys)
    }

    // key altering flatmap
    def flatMap[S:ClassTag](f: ((K,V)) => Vector[S]): (RDD[S], RDD[S]) = (light, heavy).flatMap(f)

    def createDomain[L: ClassTag](f: ((K, V)) => L): (RDD[L], RDD[L]) = (light, heavy).createDomain(f)

    // def joinDropKey[S:ClassTag](rrdd: (RDD[(K, S)], RDD[(K, S)])): (RDD[(V, S)], RDD[(V, S)]) = {
    //   if (heavyKeys.value.nonEmpty){

    //   }else (light.joinDropKey(rrdd.union), light.empty)
    // }

    def cogroupDropKey[S:ClassTag](rrdd:(RDD[(K,S)], RDD[(K,S)])): (RDD[(V, Vector[S])], RDD[(V, Vector[S])]) = {
      val runion = rrdd.union
      if (heavyKeys.value.nonEmpty){
        val rlight = runion.filter(i => !heavyKeys.value(i._1))
        val lresult = light.cogroupDropKey(rlight)

        val rheavy = runion.flatMap{
            case (k,s) => if (heavyKeys.value(k)) Vector((k, Vector(s))) else Vector()
          }.reduceByKey(_++_).collect.toMap
        val heavyRights = heavy.sparkContext.broadcast(rheavy)
        val hresult = heavy.mapPartitions(it => cogroupBy(it, heavyRights))
        (lresult, hresult)
      }else {
        val result = light.cogroupDropKey(runion)
        (result, result.empty)
      }
    }

    def cogroupDomain(dom: (RDD[K], RDD[K])): 
      (RDD[(K, Vector[V])], RDD[(K,Vector[V])], Broadcast[Set[K]]) = {
        val domain = dom.union
        if (heavyKeys.value.nonEmpty){
          val domainLight = domain.filter(l => !heavyKeys.value(l))
          val ldict = light.cogroupDomain(domainLight)

          val hdomain = domain.filter(l => heavyKeys.value(l)).collect.toSet
          val heavyDomain = heavy.sparkContext.broadcast(hdomain)
        
          val hdict = heavy.mapPartitions(it => groupBy(it, heavyDomain))
          (ldict, hdict, heavyKeys)
        }else {
          val result = light.cogroupDomain(domain)
          (result, result.empty, heavyKeys)
        }
      }

  }

  implicit class SkewPairFunctions[K: ClassTag, V: ClassTag](lrdd: (RDD[(K,V)], RDD[(K, V)])) extends Serializable {
    
    val light = lrdd._1
    val heavy = lrdd._2
    val threshold = Config.threshold
    val partitions = light.getNumPartitions

    def heavyKeys: (RDD[(K,V)], Set[K]) = {
      val lunion = lrdd.union
      (lunion, lunion.mapPartitions( it => 
        Util.countDistinct(it).filter(_._2 > threshold).iterator,true).keys.collect.toSet)
    }
    
    // def heavyKeys: (RDD[(K,V)], Set[K]) = {
    //   val lunion = lrdd.union
    //   val samples = lunion.sample(false, .05)
    //   val thresh = (samples.countApprox(1).getFinalValue().low/partitions)*0.05
    //   if (thresh < 1) (lunion, Set.empty[K])
    //   else {
    //     (lunion, samples.mapPartitionsWithIndex((index, it) => {
    //       Util.countDistinct(it).filter(_._2 > thresh).iterator
    //     }).keys.collect.toSet)
    //   }
    // }

    def cogroupDropKey[S:ClassTag](rrdd:(RDD[(K,S)], RDD[(K,S)])): (RDD[(V, Vector[S])], RDD[(V, Vector[S])]) = {
      val (lunion, hk) = lrdd.heavyKeys
      val hkeys = lunion.sparkContext.broadcast(hk)
      if (hkeys.value.nonEmpty){
        (lunion.filter(i => !hkeys.value(i._1)), lunion.filter(i => hkeys.value(i._1)), hkeys).cogroupDropKey(rrdd)
      }else {
        val result = lunion.cogroupDropKey(rrdd.union)
        (result, result.empty)
      }
    }

    def cogroupDomain(dom: (RDD[K], RDD[K])): 
      (RDD[(K, Vector[V])], RDD[(K, Vector[V])], Broadcast[Set[K]]) = {
        val (lunion, hk) = lrdd.heavyKeys
        val hkeys = lunion.sparkContext.broadcast(hk)
        if (hkeys.value.nonEmpty){
          (lunion.filter(i => !hkeys.value(i._1)), lunion.filter(i => hkeys.value(i._1)), hkeys).cogroupDomain(dom)
        }else {
          val result = lunion.cogroupDomain(dom.union)
          (result, result.empty, hkeys) 
        }
    }

  }

}