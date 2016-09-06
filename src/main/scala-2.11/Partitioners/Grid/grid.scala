package spark.example

import Skyline.{GlobalSkyline, LocalSkyline}
import org.apache.spark.{Accumulator, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by marmi on 26/5/2016.
  */

class grid(var points: RDD[point], N: Int, d: Int,
           localcount: Array[Accumulator[Int]],
           globalcount: Accumulator[Int]) extends Serializable {
  var queue = new mutable.Queue[Int]()
  var splitdim = Array.fill(N) {
    0
  }
  queue += 0
  points.cache()
  var next = 1
  while (next != N) {
    //println("next "+next)
    points = split(points, queue.head, next, splitdim(queue.head))
    points.cache()
    //    splitdim(queue.head)+=1
    //    println("dim "+ splitdim(queue.head))
    if (splitdim(queue.head) == d) {
      splitdim(queue.head) = 0
    }
    queue += queue.head
    queue += next
    queue.dequeue()
    //println("Queue "+queue)
    next += 1
  }
  //  var size=points.collect()
  //  (0 until N).foreach(x=>{
  //    println("Size"+x+"  "+size.filter(_.partition==x).size)
  //  })
  //
  //  var rdd=points.keyBy(x => {
  //    new Key(x.partition, x.sum, x.coor,x.f)
  //  }).repartitionAndSortWithinPartitions(new GridPartitioner(N)).cache()
  //  rdd.count
  //
  //  val rdd2 =rdd
  //    .mapPartitionsWithIndex((index,iter) => {
  //      //println("index "+index)
  //
  //      new LocalSkyline(d,localcount(index)).skyline(iter)
  //    }).cache()
  //  rdd2.count()
  //  val starts=System.currentTimeMillis()
  //
  //  println(" TIME " + (System.currentTimeMillis() - starts) )
  //  val s =new GlobalSkyline(d,globalcount).skyline(rdd2)


  var rdd = points.keyBy(x => {
    new Key(x.partition, x.sum, x.coor, x.f)
  }).repartitionAndSortWithinPartitions(new GridPartitioner(N)).cache()


  new GlobalSkyline(d, globalcount).skyline(rdd
    .mapPartitionsWithIndex((index, iter) => {
      val it = iter.map(_._2)
      new LocalSkyline(d, localcount(index)).skyline(it)
    }))


  //  while(true){
  //
  //  }


  def split(points: RDD[point], head: Int, next: Int, splitdim: Int): RDD[point] = {
    val minmax = points.filter(_.partition == head).aggregate(Double.MinValue, Double.MaxValue)(
      (acc, value) => (Math.max(acc._1, value.coor(0)), Math.min(acc._2, value.coor(0))),
      (acc1, acc2) => (Math.max(acc1._1, acc2._1), Math.min(acc1._2, acc2._2))
    )
    val max = minmax._1
    val min = minmax._2
    val mid = (max - min) / 2 + min

    //    println("max "+max)
    //    println("min "+min)
    //    println("mid "+mid)

    //println("size prin "+points.filter(_.partition==head).collect().size)
    val p = points.map(x => {
      if (x.partition == head) {
        if (x.coor(splitdim) >= mid) {
          x.partition = next
        }
      }

      x
    })
    //    println("size meta head"+head+" "+ p.filter(_.partition==head).collect().size)
    //    println("size meta next"+next+" "+ p.filter(_.partition==next).collect().size)
    //    println("///////////")
    //    println()
    p

  }

  class GridPartitioner(N: Int) extends Partitioner {
    require(N >= 0, s"Number of partitions ($N) cannot be negative.")

    def numPartitions = N

    def getPartition(key: Any): Int = {

      val k = key.asInstanceOf[Key]
      return k.partition

    }

  }


}
