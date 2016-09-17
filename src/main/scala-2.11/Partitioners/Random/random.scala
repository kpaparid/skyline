/**
  * Created by marmi on 24/3/2016.
  */
package spark.example

import Skyline.{GlobalSkyline, LocalSkyline}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, Partitioner}

import scala.util.Random


class random(points: RDD[point], N: Int, d: Int,
             localcount:Array[Accumulator[Int]],
             globalcount:Accumulator[Int]) extends Serializable{


//  new GlobalSkyline(d,globalcount).skyline(points.keyBy(x => {
//    new Key(x.partition, x.sum, x.coor,x.f)
//  }).repartitionAndSortWithinPartitions(new RandomPartitioner(N))
//    .mapPartitionsWithIndex((index,iter) => {
//
//      new LocalSkyline(d,localcount(index)).skyline(iter)
//    }))

  val npoints=points.keyBy(x => {
    new Key(x.partition, x.sum, x.coor,x.f)
  }).repartitionAndSortWithinPartitions(new RandomPartitioner(N)).map(_._2)
//  rdd.foreachPartition(x=>{
//    x.foreach(y=>{
//
//    })
//
//  })
//  val starts=System.currentTimeMillis()
//  new GlobalSkyline(d,globalcount).skyline(rdd
//    .mapPartitionsWithIndex((index,iter) => {
//      val it=iter.map(_._2)
//      new LocalSkyline(d,localcount(index)).skyline(it)
//    }))
//  println(" TIME " + (System.currentTimeMillis() - starts) )
//  while(true){
//
//  }



  class RandomPartitioner(N: Int) extends Partitioner {
    require(N >= 0, s"Number of partitions ($N) cannot be negative.")

    def numPartitions = N
    def getPartition(key: Any): Int = {

      Random.nextInt(numPartitions)
    }

  }
}