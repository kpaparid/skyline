///**
//  * Created by marmi on 24/6/2015.
//  */
//package Partiotioners.Grid
//
//import spark.example.point
//
//import scala.collection.mutable.ListBuffer
//
//case class partition(val id: Int, val d: Int) extends Serializable {
//
//  var points= new ListBuffer[point]
//  var splitdim=0
//  var splits=0
//
//  //  def addpoint(p:point): Unit ={
//  //    points+=p
//  //  }
//  def split(next:Int): ListBuffer[point] ={
//    splits+=1
//    var max=points.maxBy(_.coor(splitdim)).coor(splitdim)
//    var min=points.minBy(_.coor(splitdim)).coor(splitdim)
//    var mid=(max-min)/2+min
//    println("MIN      "+min)
//    println("MID      "+mid)
//    println("MAX      "+max)
//    println("///////")
//
//    var secondpoints=new ListBuffer[point]
//    var fpoints=new ListBuffer[point]
//    points.filter(x=>{
//      if(x.coor(splitdim)>mid&&x.coor(splitdim)<=max){
//        x.partition=next
//        secondpoints+=x
//        false
//      }
//      else{
//        x.partition=id
//        fpoints+=x
//        true}
//    })
//    points=fpoints
//    splitdim+=1
//    if (splitdim==d){
//      splitdim=0
//    }
//
//    return secondpoints
//  }
//
//
//}
