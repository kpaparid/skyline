case class DKey(sum: Double, dcoor: Array[Int])
  extends Ordered[DKey] {
  def compare(that: DKey): Int = that.sum compare (this.sum)
}
