case class LKey(var sum:Double,var partition: Int,var index:Long,var d:Int,var f: Array[Double],var next:Int)
  extends Ordered[LKey] {
  def compare(that: LKey): Int = this.sum compare (that.sum)
}
