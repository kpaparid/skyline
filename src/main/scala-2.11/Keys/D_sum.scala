package spark.example

case class D_sum(var sum:Double, var partition: Int, var index:Long, var d:Int, var f: Array[Double], var next:Int)
  extends Ordered[D_sum] {
  def compare(that: D_sum): Int = this.sum compare (that.sum)
}
