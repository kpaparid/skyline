package Skyline

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import spark.example.point

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created by Marmi on 9/16/2016.
  */
class RepresentativeSkyline(val d: Int, val k: Int, globalcounts: Accumulator[Int]) extends Serializable {
  var globalcount = globalcounts

  def skyline(points: RDD[point]): ListBuffer[point] = {

    var skyline: ListBuffer[point] = new ListBuffer[point]()
    val l = points.sortBy(_.sum).collect()
    val start = System.currentTimeMillis()
    l.foreach(x => {
      breakable {
        skyline.foreach(y => {
          globalcount += 1
          val l = dom(x, y)
          if (x.name == y.name) {
            break()
          }
          if (l == d) {
            println("hmm2")
            skyline -= y
          }
          else if (l == 0) {
            y.score += 1 + x.score
            println("x.score " + x.score + "\t\ty.score " + y.score)
            break()
          }
        })
        var xx = x
        skyline += xx
      }
    })
    println("skyline size "+skyline.size)
    skyline.sortBy(-_.score)
  }

  def dom(x: point, y: point): Int = {
    var l = 0
    var e = 0
    for (i <- 0 until d) {
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
    //  println("x: "+x.name+"   y: "+y.name+"   l==  "+l)
    l
  }
}