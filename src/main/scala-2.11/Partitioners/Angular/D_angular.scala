/**
  * Created by marmi on 24/3/2016.
  */
package spark.example

import Skyline.{GlobalSkyline, LocalSkyline}
import org.apache.spark.{Accumulator, Partitioner, SparkConf, SparkContext}

import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

class D_angular(var points: RDD[point], N: Int, d: Int,nmax:Int,
                localcount:Array[Accumulator[Int]],
                globalcount:Accumulator[Int]) extends Serializable{


  var k = math.pow(N, 1 /(d.toDouble - 1))
  if (k % k.toInt > 0) {
    k = k + 1
  }
  var Vc=0.0
  val pi = Math.PI

  var vol:Double =_
  var  angles= Array.ofDim[Double](d, k.toInt+1)

  def simpson(f:Double=>Double, a:Double, b:Double)=(f(a)+4*f((a+b)/2)+f(b))/6;
  def function(x: Double, n: Double) = Math.pow(Math.sin(x), n)
  type Method = (Double=>Double, Double, Double) => Double
  def integrate(f:Double=>Double, a:Double, b:Double, steps:Double, m:Method)={
    val delta:Double=(b-a)/(steps+1)
    delta*(a until b by delta).foldLeft(0.0)((s,x) => s+m(f, x, x+delta))
  }
  def calculateVol(x: Double, y: Double, n: Int): Double = {
    var volume: Double = integrate(function(_, n), x, y, 10, simpson)
    (0 to d - 2).foreach(i => {
      if (i != n){
        volume = volume * integrate(function(_, i), 0, pi / 2, 10, simpson)
      }
    }
    )
    volume
  }
  def calculateVol2(w:Double): Double ={
    //var w=Math.PI/6

    //  println(w)
    //    println("efap "+Math.tan(w))
    val L=1
    //var y=Math.tan(w) * L*L/2
    def f(x: Double) = Math.tan(w) * x

    var Vn=0.0
    if(f(L)>L){
      //      println("f(x) "+ f(L))
      val ml=f(L)-L
      val pr=ml/Math.tan(w)
      Vn=ml*pr/2
      //      println("ml "+ml)
      //      println("pr "+pr)
      //      println("Vn "+Vn)
    }
    val Vt=L*f(L)/2  -Vn
    //println("w "+w.toDegrees+"  Vt "+Vt)
    //    println("telos")
    val Vp=Vt-Vc

    return Vp


    //println("y "+y)


  }
  def bounds(btd: Double, n: Int): Double = {
    val bt = btd
    var tp=1.5707963268                 //90 Degrees

    var s_top=tp
    var s_bot=btd
    var s_mid=s_bot+(s_top-s_bot)/2




    //      println("bot "+s_bot.toDegrees+"  top "+s_top.toDegrees)
    //      println("mid "+s_mid.toDegrees)
    //      println("///////////////////")
    //println("btd "+btd, s_mid+"  n "+n)
    // println()
    //var olo=calculateVol2(s_mid)
    var olo = calculateVol(btd,s_mid, n)
    if(math.abs(olo - vol) <= 0.00001 &&olo>=vol) {
      tp=s_mid
      // println("mpika")
    }
    //println("eksw  "+olo)
    //println("vol "+vol)
    var counter=1
    while ((math.abs(olo - vol) > 0.00001 || olo < vol)&&counter!=20) {
      counter+=1
      //println("loop")
      if(olo<vol){
        //println("mikro vol")
        s_bot=s_mid
        s_mid=s_bot+(s_top-s_bot)/2

        //          println("bot "+s_bot.toDegrees+"  top "+s_top.toDegrees)
        //          println("mid "+s_mid.toDegrees)
      }
      else
      {
        //println("megalo vol")
        s_top=s_mid
        s_mid=s_top-(s_top-s_bot)/2

      }
      tp=s_mid
      //      println("BOT "+s_bot.toDegrees+"  TOP "+s_top.toDegrees )
      //      println("TP "+tp.toDegrees)
      //      println("mid "+s_mid.toDegrees)
      if (btd != tp) {
        olo = calculateVol(btd, tp, n)
        //        olo=calculateVol2(tp)
        // println("bt "+btd.toDegrees +" tp "+tp.toDegrees+" vol "+vol+" apo:"+olo)

      }
      else {
        tp = tp + tp / 2
        //println("equal: bt "+btd+" "+tp)
      }

      //
      //      println("////////////")
      //      println("////////////")
      //      println("////////////")
    }

    //println(" bt " + bt.toDegrees + " tp " + tp.toDegrees)
    var vl = calculateVol(bt, tp, n)
    //    var vl = calculateVol2(tp)
    Vc+=vl
    // println("current "+Vc)
    //println("Vol " +vl)
    //println("Gwnia "+tp.toDegrees)
    return tp
  }
  def calculate(): Unit ={



    //println("K " + k)
    //(Math.pow(L,d)/k)/4
    d match {
      case 2 =>
        vol = pi / (2 * k)

        println("Vol of each " + vol)
      case 3 =>
        vol = pi / (2 * k)
      //println("Vol of each " + vol)
      case 4 =>
        vol = pi * pi / (8 * k)
      //println("Vol of each " + vol)
      case 5 =>
        vol=pi*pi/(12*k)
      //println("Vol of each " + vol)
    }


    (0 until d-1).foreach(x=>{
      angles(x)(0)=0
      angles(x)(k.toInt)=pi/2
    })
    (0 until d-1).foreach(y=>{

      (1 until k.toInt).foreach(x=>{

        angles(y)(x)=bounds(angles(y)(x-1),y)
        println("k"+y+"  gwnia "+angles(y)(x).toDegrees)
      })
      //println()
    })

  }

  calculate()
//  split(1,0)
var nmaxs=100
  k=2
  var next=0
  (0 until k.toInt).foreach(x=>{
    (0 until d).foreach(y=>{

      points=split(nmax,(y+1)*(x+1),next,k.toInt)
      next+=1
    })
  })


  def split(nmax:Int,counter:Int,next:Int,splitdim:Int): RDD[point] ={


points.sortBy(_.f(splitdim)).zipWithIndex().map(x=>{

        if(x._2<=nmax*counter&&x._2>=nmax*(counter+1))
          x._1.partition=next
      x._1
      })
  }
  //var l=points.keyBy(x => {
  //  new Key(x.partition, x.sum, x.coor,x.f)
  //}).repartitionAndSortWithinPartitions(new AngularPartitioner(N)).foreachPartition(x=>{
  //  println("partition")
  //  x.foreach(y=>{println("sum"+y._2.sum)})
  //})
  //  def sd(p:point): point ={
  //  var g: Double = 1 / (d.toDouble - 1)
  //
  //
  //
  //  val coor = new Array[Int](d-1)
  //  var i = 0
  //  var dim=0
  //  val f = p.f
  //  //println("f "+f(0)+"   "+f(1)+"    ")
  //  f.foreach(x=>{
  //    //println("dim "+dim+"    x "+x)
  //    breakable{
  //      (0 until k.toInt).foreach(i=>{
  //        if(x>=angles(dim)(i).toDegrees&&x<=angles(dim)(i+1).toDegrees){
  //          coor(dim)=i
  //          //println("i "+i)
  //          break
  //        }
  //      })
  //    }
  //
  //
  //    dim+=1
  //  })
  //  //println("coor "+coor(0)+"   "+coor(1)+"    ")
  //  var count = 1
  //  var sum = coor(0)
  //  (1 until coor.length).foreach(x => {
  //    sum += count * coor(x) * k.toInt
  //    //println("Sum "+sum+"   x "+coor(x)+" count "+count)
  //    count += 1
  //  })
  //  if (sum > N - 1) {
  //    //println("wtf re")
  //
  //    sum = sum - N
  //  }
  //  //      println("sum "+sum)
  //  //      println("/////")
  //  //      println("/////")
  //  sum
  //
  //  val y=p
  //  y.partition=sum
  //  y
  //
  //
  //}

  //  var rdd=points.map(x=>{
  //sd(x)}).keyBy(x => {
  //    new Key(x.partition, x.sum, x.coor,x.f)
  //  }).repartitionAndSortWithinPartitions(new An(N))
  //
  //  //rdd.count
  //
  //  val rdd2 =rdd
  //    .mapPartitionsWithIndex((index,iter) => {
  ////println("index "+index)
  //
  //      new LocalSkyline(d,localcount(index)).skyline(iter)
  //    }).cache()
  //  //rdd2.count()
  //  val starts=System.currentTimeMillis()
  //
  //  println(" TIME " + (System.currentTimeMillis() - starts) )
  //  val s =new GlobalSkyline(d,globalcount).skyline(rdd2)

  var rdd=points.keyBy(x => {
    new Key(x.partition, x.sum, x.coor,x.f)
  }).repartitionAndSortWithinPartitions(new AngularPartitioner(N)).cache()
  //println(rdd.count())
  //println("eror")

  new GlobalSkyline(d,globalcount).skyline(rdd
    .mapPartitionsWithIndex((index,iter) => {
      val it=iter.map(_._2)
      new LocalSkyline(d,localcount(index)).skyline(it)
    }))


  //  while(true){
  //
  //  }





  //todo 5d
  class AngularPartitioner(N: Int) extends Partitioner {
    require(N >= 0, s"Number of partitions ($N) cannot be negative.")

    def numPartitions = N
    def getPartition(key: Any): Int = {
      //println("ey yo")
      var g: Double = 1 / (d.toDouble - 1)



      val coor = new Array[Int](d-1)
      var i = 0
      var dim=0
      val f = key.asInstanceOf[Key].f
      //println(key.asInstanceOf[Key].coor(0))

      //println("f "+f(0)+"   "+f(1)+"    ")
      f.foreach(x=>{
        //println("dim "+dim+"    x "+x)
        breakable{
          (0 until k.toInt).foreach(i=>{
            if(x>=angles(dim)(i).toDegrees&&x<=angles(dim)(i+1).toDegrees){
              coor(dim)=i
              //println("i "+i)
              break
            }
          })
        }


        dim+=1
      })
      //println("coor "+coor(0)+"   "+coor(1)+"    ")
      var count = 1
      var sum = coor(0)
      (1 until coor.length).foreach(x => {
        sum += count * coor(x) * k.toInt
        //println("Sum "+sum+"   x "+coor(x)+" count "+count)
        count += 1
      })
      if (sum > N - 1) {
        //println("wtf re")

        sum = sum - N
      }

      //  println("sum "+sum)
      //      println("/////")
      //      println("/////")
      sum
    }

  }

  class An(N: Int) extends Partitioner {
    require(N >= 0, s"Number of partitions ($N) cannot be negative.")

    def numPartitions = N

    def getPartition(key: Any): Int = {

      val k = key.asInstanceOf[Key]
      return k.partition

    }


  }


}