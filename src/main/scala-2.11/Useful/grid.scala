///**
//  * Created by marmi on 11/3/2016.
//  */
//package Partiotioners.Grid
//
//import Skyline.{GlobalSkyline, LocalSkyline}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{Partitioner, SparkContext}
//import spark.example._
//
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//
//class grid(points: RDD[point], N: Int, d: Int, sc: SparkContext) extends Serializable {
//
//
//  def getpoints(): Stream[point] = {
//
//    val start = System.currentTimeMillis()
//    val part = new Array[partition](N)
//
//    for (i <- 0 until N) {
//      part(i) = new partition(i, d)
//    }
//    //    points.foreach(x=>{
//    //      part(0).addpoint(x)
//    //    })
//    part(0).points = points.collect().to[ListBuffer] //todo fix this
//
//    var next = 1
//    var queue = new mutable.Queue[partition]()
//    queue += part(0)
//
//    while (next != N) {
//      //      part(next).points = part(queue.head.id).split().map(x=>{
//      //        x.partition=next
//      //        x
//      //      })
//      part(next).points = part(queue.head.id).split(next)
//      queue += part(queue.head.id)
//      queue += part(next)
//      queue.dequeue()
//      next += 1
//    }
//    val time = System.currentTimeMillis() - start
//    var ks = Stream.empty[point]
//    part.foreach(x => {
//      //println("Partition ID " + x.id)
//      //println("Points " + x.points.size)
//
//      ks = ks.append(x.points)
//    })
//    println("sizee "+ks.size)
//    ks
//  }
//val grid=sc.parallelize(getpoints()).keyBy(x => {
//  new Key(x.partition, x.sum, x.coor,x.f)
//}).repartitionAndSortWithinPartitions(new GridPartitioner(N))
//  .mapPartitions(iter => {
//    new LocalSkyline(d).skyline(iter)
//  }).cache()
//
//    new GlobalSkyline(d).skyline(grid)
//
//
//
//
//  //todo grid spread with point id instead of partitions
//  class GridPartitioner(N: Int) extends Partitioner {
//    require(N >= 0, s"Number of partitions ($N) cannot be negative.")
//
//    def numPartitions = N
//
//    def getPartition(key: Any): Int = {
//
//      val k = key.asInstanceOf[Key]
//      return k.partition
//
//    }
//
//    override def equals(other: Any): Boolean = other match {
//      case grid: GridPartitioner =>
//        grid.numPartitions == numPartitions
//      case _ =>
//        false
//    }
//  }
//}
