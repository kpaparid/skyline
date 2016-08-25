package spark.example

import Skyline.GlobalSkyline
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD

/**
  * Created by marmi on 24/5/2016.
  */
class default(points : RDD[point], d : Int, globalcount:Accumulator[Int]) {

  new GlobalSkyline(d,globalcount).skyline(points)

}
