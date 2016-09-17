package Skyline

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import spark.example.point

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created by Marmi on 9/16/2016.
  */
class Rlocal(val d: Int, val k: Int, lc: Accumulator[Int]) extends Serializable {
  var localcount = lc
  var local_count = 0

  def skyline(iter: Iterator[point], riter: List[point]): Iterator[point] = {
    val starts = System.currentTimeMillis()
    println("riter size "+riter.size)
    var skyline = new ListBuffer[point]()
    skyline=skyline++riter

    //println("iter size inside "+iter.size)
//    skyline.foreach(x => {
//      println(x.name + "\t" + x.partition + "\t" + x.sum + "\t\t" + x.coor(0) + "   " + x.coor(1) + "   " + x.score + "   ")
//    })
skyline=skyline.sortBy(_.sum)
    iter.foreach(x => {
     // println("x name: "+x.name)
      breakable {
        skyline.foreach(y => {


          if (y.name == x.name) {
            break()
          }
          //println("comp x: "+x.name+"\ty:"+y.name)
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
        println("\t\tbazw "+x.name)
        skyline += x
      }
    })




//    iter.foreach(x=>{
//      breakable{
//      riter.foreach(y=>{
//        println("comp x: "+x.name+"\ty:"+y.name)
//        if(x.name==y.name) {
//        break()
//        }
//
//          val l=dom(x,y)
//          if(l==0){
//            print("bazw "+x.name)
//            println("\ty "+y.name)
//            skyline+=x
//            break()
//          }
//
//      })
//      }
//    })
    println("Number of extra points "+skyline.size)
    skyline.sortBy(_.score).toIterator
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