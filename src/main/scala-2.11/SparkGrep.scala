package spark.example

import Skyline._
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD


object SparkGrep {
  def main(args: Array[String]) {
    //
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("Skyline")
      //      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      ///
      //          conf.registerKryoClasses(Array(
      //            classOf[point]
      //            , classOf[scala.collection.mutable.ListBuffer[point]], classOf[spark.example.Key]
      //            //, classOf[double]
      //          ))
      //////////////////
      //      conf.set("spark.kryo.registrationRequired", "true")
      .set("spark.executor.memory", "4g")
      //.set("spark.eventLog","enabled")
      .set("spark.driver.memory", "4g")
      .set("spark.rdd.compress", "true")
      .set("spark.driver.cores", "4")
      .set("spark.akka.frameSize", "300")

    //.set("spark.eventLog.enabled","true")
    val sc = new SparkContext(conf)
    //sc.setLocalProperty("spark.scheduler.pool", "pool1")

    /**
      * Method File N d
      */
    //


    val method = "Grid" //args(0).toString

    val N = 2 //args(2).toInt
    val d = 2 //args(3).toInt
    //val points = sc.broadcast(input.points)
    //git reset --soft HEAD^
    //    val points=  sc.textFile("h10m2d.txt", N).map(x => {
    //
    //      var coor = new Array[String](d)
    //      coor=x.split(" ")
    //      //println(coor(1))
    //      var sub=coor.slice(1,d+1).map(_.toDouble)
    //
    //      // coor = x.split(" ").map(_.toDouble)
    //      new point(coor(0),sub, d)
    //    })
    //
    ///
    val data = "Data/"
    var file = data+"uni1m4d.txt"
//    file="hdfs://localhost:9000/in/7_2d.txt"
    val pointss = sc.textFile(file, N).zipWithIndex().map(y => {
      val x = y._1
      //      var coor = new Array[String](d)
      val subs = x.split(" ").map(_.toInt)
      val sub = subs.map(_.toDouble)
      //println(coor(1))
      //var sub=coor.slice(1,d+1).map(_.toDouble)

      // coor = x.split(" ").map(_.toDouble)
      new point(y._2.toString, sub, d)
    })

    var points = sc.parallelize(pointss.take(100))
    //    points.sortBy(_.f(0)).foreach(x=>{
    //     // println("f "+x.f(0)+"\tcoor  "+x.coor(0)+" "+x.coor(1)+"\t"+x.sum)
    //    })
    //    points.cache()
    //      val points= pointss


    //println("Points "+points.count())
    //    var splitdim=0
    //    val min=points.reduce((x,y)=>{
    //      if(x.coor(splitdim)>y.coor(splitdim))
    //        Tuple2(y,x)
    //      else
    //        x
    //    }).coor(splitdim)

    val starts = System.currentTimeMillis()
    val k = 1

    (0 until k).foreach(x => {

      var localcount = Array.fill[Accumulator[Int]](N)(sc.accumulator(0, "local"))
      var globalcount = sc.accumulator(0, "global")
      method match {

        case "Grid" => {
          println("Grid Method")


          val npoints = new grid(points, N, d, localcount, globalcount)
          points = npoints.npoints

        }
        case "Dyn Angular" => {
          //println("Angular Method")
          val current = sc.accumulator(0)
          val next = sc.accumulator(0)
          val npoints = new dyn_angular(points, N, d, next, current, localcount, globalcount)
          points = npoints.npoints
          //new angular(points, N, d, localcount,globalcount)
        }
        case "Random" => {
          //println("Random Method")
          val npoints = new random(points, N, d, localcount, globalcount)
          points = npoints.npoints
        }
        case "None" => {
          println("Non Parallel Method")
          //new default(points,d,globalcount)

        }
        case "Partitioned" => {
          //          println("Already Partitioned Data")
          //          val partitioned=points //.coalesce(1)
          //            .keyBy(x => {
          //            new Key(x.partition, x.sum, x.coor,x.f)
          //          }).mapPartitions(iter => {
          //            new LocalSkyline(d,sc.accumulator(0)).skyline(iter)
          //          }).cache()
          //          new GlobalSkyline(d,sc).skyline(partitioned)

        }
        case _ => {
          println("Incorrect Method")
          System.exit(1)
        }
      }
      val k = 2
      val rdd = points.mapPartitionsWithIndex((index, iter) => {
        val l = new LocalSkyline(d, localcount(index)).skyline(iter)
        //println("iter size "+l)
        l
      }).cache()
      representativeskyline(rdd)

      def representativeskyline(rdd: RDD[point]): Unit = {
        val skyline = rdd.mapPartitions(it => {
          val ite = it.toList.sortBy(-_.score).take(k).toIterator
          ite
        })
        skyline.foreach(x => {
          println(x.name + "\t" + x.partition + "\t" + x.sum + "\t\t" + x.coor(0) + "   " + x.coor(1) + "   " + x.score + "   ")
        })
        println("/////////////end of local///////////////")
        val s = new RepresentativeSkyline(d, k, globalcount).skyline(skyline)
        s.foreach(x => {
          println(x.name + "\t" + x.partition + "\t" + x.sum + "\t\t" + x.coor(0) + "   " + x.coor(1) + "   " + x.score + "   ")
        })
        println("////////////////////starting local////////////////")
        rdd.collect()
        val Rlocal = rdd.mapPartitions(iter => {
                 // println("next iter "+iter.size)
          println("##############next Part $$$$$$$$$$$$$$$$")
          val it = new Rlocal(d, k, globalcount)

          it.skyline( iter,s.sortBy(_.sum).toList)

        }).cache()
        Rlocal.count()
        println("////////////////////ending local ///////////////////")
        println("////////////////////starting final ///////////////////")
        val Reskyline = Rlocal.collect() ++ s
//        skyline.filter(_.name==43).take(1).foreach(x=>{println(x.name)})
//        skyline.collect().foreach(x => {
//          if(x.score==1)
//          println(x.name + "\t" + x.partition + "\t" + x.sum + "\t\t" + x.coor(0) + "   " + x.coor(1) + "   " + x.score + "   ")
//        })
//        println("/s")
        Reskyline.foreach(x => {
          println(x.name + "\t" + x.partition + "\t" + x.sum + "\t\t" + x.coor(0) + "   " + x.coor(1) + "   " + x.score + "   ")
        })
        println("/\t/\t")
        val fskyline=new RGlobal(d,globalcount)
        fskyline.skyline(Reskyline)

        println("////////////////////ending final ///////////////////")
      }


      //Rlocal.collect()


      //      Rlocal.foreach(x => {
      //        println(x.name+"\t"+x.sum+"\t\t" + x.coor(0)+"   "+ x.coor(1)+"   "+ x.score+"   ")
      //      })

      //      new GlobalSkyline(d, globalcount).skyline(points
      //        .mapPartitionsWithIndex((index, iter) => {
      //          new LocalSkyline(d, localcount(index)).skyline(iter)
      //        }))
    })



    println("FINAL TIME " + (System.currentTimeMillis() - starts) / k)

  }
}

