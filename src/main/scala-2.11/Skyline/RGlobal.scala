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

class RGlobal(var d: Int,globalcounts:Accumulator[Int]) extends Serializable {
  var globalcount = globalcounts
  def skyline(points: Array[point]) = {

    var skyline: ListBuffer[point] = new ListBuffer[point]()
    val l = points.sortBy(_.sum)
    val start = System.currentTimeMillis()
    l.foreach(x => {
      breakable {
        skyline.foreach(y => {
          if (y.name == x.name) {
          break()
          }

            globalcount += 1
            val l = dom(x, y)
            if (l == d) {
              println("hmm2") //todo na bgei auto
              //println("bggazw "+y._2.name)
              skyline -= y
            }
            else if (l == 0) {
              y.score += 1 + x.score
              break()
            }

        })
        skyline += x
      }
    })

    skyline.sortBy(-_.score).foreach(x => {
      println(x.name+"\t\t" + x.coor(0)+"   "+ x.coor(1)+"   "+ x.score+"   ")
    })
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
    l
  }
}
