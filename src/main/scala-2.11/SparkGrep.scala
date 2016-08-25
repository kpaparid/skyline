package spark.example
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


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


    val method = "Angular" //args(0).toString

    val N = 8 //args(2).toInt
    val d = 4 //args(3).toInt
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
    //
    val data="Data/"
    val pointss=  sc.textFile(data+"uni1m4d.txt", N).zipWithIndex().map(y => {
      val x = y._1
//      var coor = new Array[String](d)
      val subs=x.split(" ").map(_.toInt)
      val sub=subs.map(_.toDouble)
      //println(coor(1))
      //var sub=coor.slice(1,d+1).map(_.toDouble)

      // coor = x.split(" ").map(_.toDouble)
      new point(y._2.toString,sub, d)
    })

    val points=sc.parallelize(pointss.take(16))
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

      var localcount=Array.fill[Accumulator[Int]](N)(sc.accumulator(0,"local"))
      var globalcount=sc.accumulator(0,"global")
      method match {

        case "Grid" => {
          println("Grid Method")


          new grid(points,N,d,localcount,globalcount)
//           localcount=Array.fill[Accumulator[Int]](N)(sc.accumulator(0,"local"))
//           globalcount=sc.accumulator(0,"global")
//
//          println("angular")
//          new angular(points, N, d, localcount,globalcount)
//           localcount=Array.fill[Accumulator[Int]](N)(sc.accumulator(0,"local"))
//           globalcount=sc.accumulator(0,"global")
//          println("random")
//          new random(points,N,d,localcount,globalcount)
        }
        case "Angular" => {
          //println("Angular Method")
          var current=sc.accumulator(0)
          var next=sc.accumulator(0)
          new dyn_angular(points,N,d,next,current,localcount,globalcount)
          //new angular(points, N, d, localcount,globalcount)
        }
        case "Random" => {
          //println("Random Method")
         new random(points,N,d,localcount,globalcount)

        }
        case "None" => {
          println("Non Parallel Method")
          new default(points,d,globalcount)
        }
        case "Partitioned"=>{
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
    })

    println("FINAL TIME " + (System.currentTimeMillis() - starts) / k)

  }
  }

