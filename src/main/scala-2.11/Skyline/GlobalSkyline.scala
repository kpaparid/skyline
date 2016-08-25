package Skyline

import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import spark.example.{Key, point}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created by marmi on 28/4/2016.
  */

//todo sort global

class GlobalSkyline(var d: Int,globalcounts:Accumulator[Int]) extends Serializable {
  var globalcount = globalcounts
  def skyline(points: RDD[point]) = {

points.count()
    var skyline: ListBuffer[point] = new ListBuffer[point]()
    val l = points//.sortBy(_.sum,true).cache()
       //(Ordering.by[(Key, point), Double](_._1.sum))

        .sortBy(_.sum)
    .collect()
    val start = System.currentTimeMillis()
    l.foreach(x => {
      breakable {
        skyline.foreach(y => {
          //if(y.name!=x.name){
          globalcount += 1
            val l = dom(x, y)
            if (l == d) {
              println("hmm2") //todo na bgei auto
              //println("bggazw "+y._2.name)
              skyline -= y
            }
            else if (l == 0) {
              break()
            }


        })
        //println("bazw "+x._2.name)
        skyline += x
      }
    })
//    l.foreach(x=>{
//      println(x.name+"\t\t" + x.sum)
//    })
//    println("RESULTS")
println()
    println("GLOBAL TIME " + (System.currentTimeMillis() - start))
    println("GLOBAL SIZE " + skyline.size)
   println("GLOBAL COUNT " + globalcount)
//    skyline.foreach(x => {
//      println(x.name+"\t\t" + x.coor(0)+"   "+ x.coor(1)+"   "+ x.sum+"   ")
//    })
  }

  def dom(x: point, y: point): Int = {

    var l = 0
    var e = 0

    //println("              x: "+x.name+"   y: "+y.name)
    for (i <- 0 until d) {
      //println("              x: "+x.coor(i)+"   y: "+y.coor(i))
      if (x.coor(i) < y.coor(i)) {
        l += 1
      }
      if (x.coor(i) == y.coor(i)) {
        e += 1
      }
    }

    if (l != 0) {
      l = l + e
    }
    if (e == d) {
      l = 1
    }
//println("l "+l)
    l

  }

}
