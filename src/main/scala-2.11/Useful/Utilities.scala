import Skyline.LocalSkyline
import org.apache.spark.rdd.RDD
import spark.example

//package Useful
//
//import spark.example.point
//
///**
//  * Created by marmi on 28/4/2016.
//  */
//class Utilities {
////  def quicksort(s: Stream[point]): Stream[point] = {
////    if (s.size < 2) s
////    else {
////      val (pivot #:: rest) = s
////      val (less, greater) = rest.partition(_.sum < pivot.sum)
////      quicksort(less) append (pivot #:: quicksort(greater))
////    }
////  }
//
//
//  //  def calculateL: Double ={
//  //    var L:Double=0
//  //    var max:Double=0
//  //    (0 until d).foreach(dim=>{
//  //      max=points.map(x=>x.coor(dim)).max()
//  //      if(L<max)
//  //        L=max
//  //    })
//  //    return L
//  //
//  //  }
//  //var L=5//calculateL
//
//// integrals
////  def leftRect(f:Double=>Double, a:Double, b:Double)=f(a)
////  def midRect(f:Double=>Double, a:Double, b:Double)=f((a+b)/2)
////  def rightRect(f:Double=>Double, a:Double, b:Double)=f(b)
////  def trapezoid(f:Double=>Double, a:Double, b:Double)=(f(a)+f(b))/2
//
//
//
//
//
//
//
//
//
//
//
//  //  def comp ={
//  //    val test1= Source.fromFile("results1.txt")
//  //    val test2= Source.fromFile("results2.txt")
//  //    var t1 = new ListBuffer[String]()
//  //    var t2 = new ListBuffer[String]()
//  //    var dif1=""
//  //    var dif2=""
//  //    var line2=test2.getLines
//  //    for (line1 <- test1.getLines) {
//  //      t1+=line1
//  //    }
//  //    for (line2 <- test2.getLines) {
//  //      t2+=line2
//  //    }
//  //    var c=0
//  //    t1.foreach(x1=>{
//  //      var x2=t2(c)
//  //      if(x1>x2){
//  //        println("x1 "+x1+"   x2 "+x2)
//  //        c+=1
//  //      }
//  //      if(x1<x2){
//  //        println("x1 "+x1+"   x2 "+x2)
//  //        c-=1
//  //      }
//  //      c+=1
//  //    }
//  //    )
//  //    var er1=points.filter(_.id==354495)
//  //    var er2=points.filter(_.id==369228)
//  //    println(" er1 "+er1.take(1).foreach(x=>{
//  //      println(" coor "+x.coor(0)+"   "+x.coor(1)+"   "+x.coor(2))
//  //    }))
//  //    println(" er2 "+er2.take(1).foreach(x=>{
//  //      println(" coor "+x.coor(0)+"   "+x.coor(1)+"   "+x.coor(2))
//  //    }))
//  //  }
//
////  def none(): RDD[(Key, point)] = {
////    sc.parallelize(points.value).keyBy(x => {
////      new Key(x.partition, x.sum, x.coor,x.f)
////    }).cache()
////  }
////  def random(): RDD[(Key, point)] = {
////    sc.parallelize(points.value).keyBy(x => {
////      new Key(x.partition, x.sum, x.coor,x.f)
////    }).repartitionAndSortWithinPartitions(new RandomPartition(N))
////      .mapPartitions(iter => {
////        new LocalSkyline(d).skyline(iter)
////      }).cache()
////  }
////  def angle(): RDD[(Key, point)] = {
////    sc.parallelize(points.value).keyBy(x => {
////      new Key(x.partition, x.sum, x.coor,x.f)
////    }).repartitionAndSortWithinPartitions(new AngularPartition(N, d))
////      .mapPartitions(iter => {
////        new LocalSkyline(d).skyline(iter)
////      }).cache()
////  }
////  def grid(): RDD[(Key, point)] = {
////    sc.parallelize(new GridPartitioning2(points.value, N, d).getpoints()).keyBy(x => {
////      new Key(x.partition, x.sum, x.coor,x.f)
////    })
////      .repartitionAndSortWithinPartitions(new GridPartition(N))
////      .mapPartitions(iter => {
////        new LocalSkyline(d).skyline(iter)
////      }).cache()
////  }
//
//  //    def grid2(): RDD[(Key, point)] = {
//  //
//  //      val ss = System.currentTimeMillis()
//  //      val s = points.toLocalIterator.to[ListBuffer]
//  //      println("eksw time " + (System.currentTimeMillis() - ss))
//  //      sc.parallelize(new Grid(s, N, d).getpoints()).keyBy(x => {
//  //        new Key(x.partition, x.sum, x.coor,x.f)
//  //      })
//  //        .repartitionAndSortWithinPartitions(new GridPartition(N))
//  //        .mapPartitions(iter => {
//  //          new LocalSkyline(d).skyline(iter)
//  //        }).cache()
//  //    }
//def none2(): RDD[(Key, point)] = {
//points //.coalesce(1)
//  .keyBy(x => {
//  new Key(x.partition, x.sum, x.coor,x.f)
//}).sortByKey(true)
//
//}
//
////
//////
////def random2(): RDD[(Key, point)] = {
////  points.keyBy(x => {
////    new Key(x.partition, x.sum, x.coor,x.f)
////  }).repartitionAndSortWithinPartitions(new RandomPartition(N))
////    .mapPartitions(iter => {
////      new LocalSkyline(d).skyline(iter)
//    }).cache()
//}
////
////
////
//
//
//
//
//
//}
























