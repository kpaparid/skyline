package Skyline

import org.apache.spark.{Accumulator, SparkContext}
import spark.example.{Key, point}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

class LocalSkyline(var d: Int,lc: Accumulator[Int]) extends Serializable {

  var localcount = lc
  var local_count =0
  def skyline(iter: Iterator[point]): Iterator[ point] = {

    val starts=System.currentTimeMillis()

    var skyline = new ListBuffer[point]()


    iter.foreach(x => {
     // println("lp "+x._2.sum+" f "+x._2.f(0)+" "+x._2.coor(0)+"\t"+x._2.coor(1))

      local_count += 1
      localcount += 1
      breakable {
        skyline.foreach(y => {


          //if(y.name!=x._2.name){
          localcount += 1
          val l = dom(x, y)
          if (l == d) {
            println("hmm")
            skyline -= y
          }
          else if (l == 0) {
            break()
          }


      })

        skyline += x
        //skyline=skyline.sortBy(_.sum)//(Ordering[Double])
      }
    })
//        println("\nLocal points "+local_count)
//        println("\nLocal Count " + localcount)
//    println("\nLOCAL SIZE " + skyline.size)



//println()
//    println("\n")
//    println(" Inside time " + (System.currentTimeMillis() - starts) )
//    print("points  ")
//    print(local_count)
//    print("comp  ")
//    print(" ")
//    print(  localcount)
//    print("size  ")
//    print(" ")
//    println( skyline.size)
////println()
//    println("//////////////")
    skyline.toIterator
  }

  def dom(x: point, y: point): Int = {

    var l = 0
    var e = 0


//    println("comparing sum " +x.sum+ " with "+ y.sum)
    //println("\t"+localcount)
    for (i <- 0 until d) {
//      println("x: "+x.coor(i)+"\ny: "+y.coor(i)+"\n")
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