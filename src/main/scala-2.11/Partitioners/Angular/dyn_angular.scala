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
    .repartitionAndSortWithinPartitions(new an(N,cu,k))
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
        }).repartitionAndSortWithinPartitions(new lan(N,cu,k))
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
        }).repartitionAndSortWithinPartitions(new an(N,cu,k))
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



class lan(N: Int,current:Int,k:Int) extends Partitioner {
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


  class an(N: Int,current:Int,k:Int) extends Partitioner {
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

//println("mesa "+partition)
      partition
    }
  }
//  (0 until d-1).foreach(dd=>{
//println("new dimension")
//    points=points.sortBy(_.f(dd)).zipWithIndex().map(x=>{
//      var c=n/k
//      var index=x._2%c.toInt
////      println("index "+index)
////      var c=(n/Math.pow(k,d+1))
//
//
////      var c=n/next
////      println("mesa "+k)
//var p=(x._2/c).toInt
//      var kc=k/(dd+1)
//var part=x._1.partition+p*kc
//
//      println("index "+x._2+"\t p "+p+"\t old_partition "+x._1.partition+"\t new_partition "+part+"\t current "+kc)
//      x._1.partition= part
//      x._1
//    }).cache()
////    kc=kc-1
//
//
//    points.count()
////    current.setValue(150)
////    next.setValue(100)
//////    c=c/k.toInt
////    println("next "+next)
////    println("current "+current)
//  })
//
//  points.foreach(x=>{
//    println(x.f(0)+"\t"+x.f(1)+"\t"+x.partition)
////    println(x.f(0)+"\t"+x.partition)
//  })
//  var rdd=points.keyBy(x => {
//    new DKey(x.sum,x.coor_p)
//  }).repartitionAndSortWithinPartitions(new Angular(N)).cache()
//  .count()

//  var local=rdd
//    .mapPartitionsWithIndex((index,iter) => {
//
//      new LocalSkyline(d,localcount(index)).skyline(iter)
//    })
//  new GlobalSkyline(d,globalcount).skyline(local)



//
//  class Angular(N: Int) extends Partitioner {
//    require(N >= 0, s"Number of partitions ($N) cannot be negative.")
//
//    def numPartitions = N
//
//    def getPartition(key: Any): Int = {
//      val Key=key.asInstanceOf[DKey]
//      var g: Double = 1 / (d.toDouble - 1)
//
//
//
//      val coor = Key.dcoor
//
//      //println(key.asInstanceOf[Key].coor(0))
//
//      //println("f "+f(0)+"   "+f(1)+"    ")
//
//      //println("coor "+coor(0)+"   "+coor(1)+"    ")
//      var count = 1
//      var sum = coor(0)
//      (1 until coor.length).foreach(x => {
//        sum += count * coor(x) * k.toInt
//        //println("Sum "+sum+"   x "+coor(x)+" count "+count)
//        count += 1
//      })
//      if (sum > N - 1) {
//        //println("wtf re")
//
//        sum = sum - N
//      }
//
//       // println("sum "+sum)
//      //      println("/////")
//      //      println("/////")
//      sum
//
//    }
//
//
//  }
//}
