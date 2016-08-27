package spark.example

import Skyline.{GlobalSkyline, LocalSkyline}
import org.apache.spark.{Accumulator, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

class dyn_angular(var points: RDD[point], N: Int, d: Int,next:Accumulator[Int],var current:Accumulator[Int],
              localcount:Array[Accumulator[Int]],
              globalcount:Accumulator[Int]) extends Serializable{



  var total=points.count()
  var k_d = math.pow(N, 1 /(d.toDouble - 1))
  if (k_d % k_d.toInt > 0) {k_d = k_d + 1}
  var k=k_d.toInt
  println("k\t"+k)
  println("total\t"+total)
var cu=(total/k).toInt

//  var ne=Math.pow(2,it)
  var dim=0
var npoints=points.sortBy(_.f(0)).zipWithIndex()


  .keyBy(x=> {

    val partition = x._1.partition
    val index = x._2
    val f = x._1.f
    val next = (N / Math.pow(k, 1)).toInt
  new NKey(partition,index,dim+1,f,next)
})
    .repartitionAndSortWithinPartitions(new index_partitioner(N,cu,k))
    .mapPartitionsWithIndex((x,y)=>{
      y.map(k=>{
        k._2._1.partition=x
        k._2._1
      })
    })

//npoints.foreachPartition(p=>{
//
//  println("\nnew partition\nnumber of points "+p.size+"\n")
//
////  println(p.foreach(x=>{
////    println("f "+x.f(0)+" "+x.f(1)+" "+x.f(2)+"\t"+x.partition)
////  }))
//
//})

  npoints=npoints.mapPartitions(iter=>{

    iter.zipWithIndex.map(x=>{
      x._1.spot=x._2
      x._1
    })

  })
  (1 until d-1).foreach(it=>{

      cu=(total/ Math.pow(k, it + 1)).toInt

      if(it==d-2){
        //println("final round")
        npoints=npoints.keyBy(x=> {
          val sum=x.sum
          val partition = x.partition
          val index = x.spot
          val f = x.f
          val next = (N / Math.pow(k, it + 1)).toInt
          //println("next "+cu)
          new LKey(sum,partition,index,it+1,f,next)
        }).repartitionAndSortWithinPartitions(new sum_partitioner(N,cu,k))
          .mapPartitionsWithIndex((x,y)=>{
            y.map(k=>{
              k._2.partition=x
              k._2.spot=0
              k._2

            })
          })
        npoints=npoints.mapPartitions(iter=>{
          iter.zipWithIndex.map(x=>{
            x._1.spot=x._2
            x._1
          })
        })
      }
    else{//
          npoints=npoints.keyBy(x=> {
          val partition = x.partition
          val index = x.spot
          val f = x.f
          val next = (N / Math.pow(k, it + 1)).toInt
          new NKey(partition,index,it+1,f,next)
        }).repartitionAndSortWithinPartitions(new index_partitioner(N,cu,k))
        .mapPartitionsWithIndex((x,y)=>{
          y.map(k=>{
            k._2.partition=x
            k._2.spot=0
            k._2
          })
        })
        npoints=npoints.mapPartitions(iter=>{
          iter.zipWithIndex.map(x=>{
            x._1.spot=x._2
            x._1
          })
        })
      }

//
//    println("\n/////////////////\n\tROUND "+(it+1)+"\n/////////////////\n")
//    npoints.foreachPartition(p=>{
//
//      println("new partition")
////      println("number of points "+ p.size)
//      println(p.foreach(x=>{
//       // println("f "+x.f(0)+" "+x.f(1)+" "+x.f(2)+"\t"+x.partition+"\t"+x.sum)
//      }))
//
//  })

    new GlobalSkyline(d,globalcount).skyline(npoints
      .mapPartitionsWithIndex((index,iter) => {

        new LocalSkyline(d,localcount(index)).skyline(iter)
      }))

  })



}



class sum_partitioner(N: Int, current:Int, k:Int) extends Partitioner {
  require(N >= 0, s"Number of partitions ($N) cannot be negative.")

  def numPartitions = N

  def getPartition(key: Any): Int = {

    val Key = key.asInstanceOf[LKey]
    var partition = Key.partition
    val index = Key.index
    var f = Key.f
    var dim=Key.d
    val next = Key.next
    val p = index / current
    partition+=p.toInt*next
    partition
  }
}


  class index_partitioner(N: Int, current:Int, k:Int) extends Partitioner {
    require(N >= 0, s"Number of partitions ($N) cannot be negative.")

    def numPartitions = N
    def getPartition(key: Any): Int = {
      val Key = key.asInstanceOf[NKey]
      var partition = Key.partition
      val index = Key.index
      var f = Key.f
      var dim=Key.d
      val next = Key.next
//      println("n "+next)
//      println("i "+index)
//      println("c "+current)
//      println("p "+partition)
//      if (index >= current) {
      //        partition += next.toInt
      //      }
      val p = index / current
      partition+=p.toInt*next
      partition
    }
  }

