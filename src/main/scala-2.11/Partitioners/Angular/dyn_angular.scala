package spark.example

import Skyline.{GlobalSkyline, LocalSkyline}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, Partitioner}

class dyn_angular(var points: RDD[point], N: Int, d: Int, next: Accumulator[Int], var current: Accumulator[Int],
                  localcount: Array[Accumulator[Int]],
                  globalcount: Accumulator[Int]) extends Serializable {

  var NN=0
  var total = points.count()
  var k_d = math.pow(N, 1 / (d.toDouble - 1))
  if (k_d % k_d.toInt > 0) {
    NN=N-Math.pow(k_d.toInt,d-1).toInt
  println("d-1 "+(d-1))
    println("k_d "+k_d.toInt)
  }
  println("NN "+NN)
  var k = k_d.toInt
//  var cu = (total / k).toInt
  var cu = ((N-NN)*total/(N*k)).toInt
  println("cu \t" + cu)
  println("k\t" + k)
  println("total\t" + total)
  println("dsd " + (d - 1))

  var dim = 0
  var npoints = points

  var iterations=d-1
  if(k==1){
    iterations=1
    npoints=npoints.sortBy(_.f(0)).zipWithIndex().keyBy(x => {
      val sum=x._1.sum
      val partition = x._1.partition
      val index = x._2
      val f = x._1.f
      val next = ((N-NN) / Math.pow(k, 1)).toInt
      //println("next "+cu)
      new D_sum(sum, partition, index, dim + 1, f, next)
    }).repartitionAndSortWithinPartitions(new sum_partitioner(N, cu, k,NN))
      .mapPartitionsWithIndex((x, y) => {
        y.map(k => {
          k._2._1.partition = x
          k._2._1
        })
      })
  }
  else{
    npoints=npoints.sortBy(_.f(0)).zipWithIndex().keyBy(x => {
      val partition = x._1.partition
      val index = x._2
      val f = x._1.f
      val next = ((N-NN) / Math.pow(k, 1)).toInt
      //println("next "+next)
      new D_index(partition, index, dim + 1, f, next)
    }).repartitionAndSortWithinPartitions(new index_partitioner(N, cu, k,NN))
      .mapPartitionsWithIndex((x, y) => {
        y.map(k => {
          k._2._1.partition = x
          k._2._1
        })
      })

  }





  npoints.foreachPartition(p => {
    println("new partition\nnumber of points " + p.size)
    //  println(p.foreach(x=>{
    //    println("f "+x.f(0)+" "+x.f(1)+" "+x.f(2)+"\t"+x.partition)
    //  }))
  })

  npoints = npoints.mapPartitions(iter => {

    iter.zipWithIndex.map(x => {
      x._1.spot = x._2
      x._1
    })

  })

  (1 until iterations).foreach(it => {

    var c = cu.toDouble/k
        if((c-c.toInt)>0){
        c+=1
        }
     cu=c.toInt
    println("cu " + cu)
    //println("d-2 "+ (d-2))


    /**
      * next: step of splitting
      */

    if (it == d - 2) {
      println("\n/////////////////\n\tFINAL ROUND\n/////////////////")

      npoints = npoints.keyBy(x => {
        val sum = x.sum
        val partition = x.partition
        val index = x.spot
        val f = x.f
        val next = ((N - NN) / Math.pow(k, it + 1)).toInt
        //println("next "+cu)
        new D_sum(sum, partition, index, it + 1, f, next)
      }).repartitionAndSortWithinPartitions(new sum_partitioner(N, cu, k,NN))
        .mapPartitionsWithIndex((x, y) => {
          y.map(k => {
            k._2.partition = x
            k._2.spot = 0
            k._2
          })
        })
      npoints = npoints.mapPartitions(iter => {
        iter.zipWithIndex.map(x => {
          x._1.spot = x._2
          x._1
        })
      })
    }
    else {
      npoints = npoints.keyBy(x => {
        val partition = x.partition
        val index = x.spot
        val f = x.f
        val next = ((N - NN) / Math.pow(k, it + 1)).toInt
        new D_index(partition, index, it + 1, f, next)
      }).repartitionAndSortWithinPartitions(new index_partitioner(N, cu, k,NN))
        .mapPartitionsWithIndex((x, y) => {
          y.map(k => {
            k._2.partition = x
            k._2.spot = 0
            k._2
          })
        })
      npoints = npoints.mapPartitions(iter => {
        iter.zipWithIndex.map(x => {
          x._1.spot = x._2
          x._1
        })
      })
    }

    //
    println("\n/////////////////\n\tROUND " + (it + 1) + "\n/////////////////\n")
    npoints.foreachPartition(p => {

//      println("new partition")
      println("number of points " + p.size)
      //      println(p.foreach(x=>{
      //        println("f "+x.f(0)+" "+x.f(1)+" "+x.f(2)+"\t"+x.partition+"\tsum "+x.sum)
      //      }))

    })
    println()
  })
  new GlobalSkyline(d, globalcount).skyline(npoints
    .mapPartitionsWithIndex((index, iter) => {
      new LocalSkyline(d, localcount(index)).skyline(iter)
    }))

}


class sum_partitioner(N: Int, current: Int, k: Int,NN:Int) extends Partitioner {
  require(N >= 0, s"Number of partitions ($N) cannot be negative.")

  def numPartitions = N

  def getPartition(key: Any): Int = {

    val Key = key.asInstanceOf[D_sum]
    var partition = Key.partition
    val index = Key.index
    var f = Key.f
    var dim = Key.d
    val next = Key.next
    var p = index / current
    //    if((p-p.toInt)>0){
    //      p+=1
    //    }
    //    println("index "+index)
    //        println("p "+p)
    //    println("current "+current)

    if (p == k) {
      println("mpika")

      partition = N-NN //todo fix this
    }
    else {
      partition += p.toInt * next
    }
//    println("\nnext " + next)
//        println("cu "+current)
//    println("index " + index)
//    println("p " + p)
//    println("part " + partition)
    if(partition>=N){
      return N-1
    }
    partition
  }
}
class index_partitioner(N: Int, current: Int, k: Int,NN:Int) extends Partitioner {
  require(N >= 0, s"Number of partitions ($N) cannot be negative.")

  def numPartitions = N

  def getPartition(key: Any): Int = {
    val Key = key.asInstanceOf[D_index]
    var partition = Key.partition
    val index = Key.index
    var f = Key.f
    var dim = Key.d
    val next = Key.next
    //      println("n "+next)
    //      println("i "+index)
    //      println("c "+current)
    //      println("p "+partition)
    //      if (index >= current) {
    //        partition += next.toInt
    //      }
    var p = index / current
    //      println("index "+index)
    //      println("current "+current)
    //      if((p-p.toInt)>0){
    //        p+=1

    if (p == k) {
//println("mpika2")
      partition = N-NN //todo fix this
      //println("p "+partition)
    }
    else {
      partition += p.toInt * next
    }
    //      println("p "+p)
    if(partition>=N){
      return N-1
    }
    partition

  }
}

