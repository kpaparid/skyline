package spark.example

case class D_index(var partition: Int, var index:Long, var d:Int, var f: Array[Double], var next:Int)
  extends Ordered[D_index] {
  def compare(that: D_index): Int = this.f(d) compare (that.f(d))
}
