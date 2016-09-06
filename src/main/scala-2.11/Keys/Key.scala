case class Key(partition: Int, sum: Double, coor: Array[Double],f: Array[Double])
  extends Ordered[Key] {
  def compare(that: Key): Int = this.sum compare (that.sum)
}
