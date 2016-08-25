package spark.example

case class NKey(var partition: Int,var index:Long,var d:Int,var f: Array[Double],var next:Int)
  extends Ordered[NKey] {
  def compare(that: NKey): Int = this.f(d) compare (that.f(d))
}