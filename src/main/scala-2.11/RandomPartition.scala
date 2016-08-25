package spark.example

import org.apache.spark.Partitioner
import scala.util.Random

class RandomPartition(N: Int) extends Partitioner {

  def numPartitions = N

  def getPartition(key: Any): Int = {
    Random.nextInt(numPartitions)
  }
}
