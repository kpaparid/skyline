package spark.example



case class point(var name:String, var coor: Array[Double], d: Int)
  extends Serializable {

  var coor_p=new Array[Int](d-1)
  var f=new Array[Double](d-1)
  var sum=0.0
  var partition=0
  var spot=0
  def suma() {
    coor.foreach(x=> {
      sum = sum + x
    })
    }
  def angles(): Unit ={
    for(j<-0 until d-1) {

      var s=0.0
      for(k<-j+1 until d) {
        s=s+coor(k)*coor(k)
        //println("S "+s)
      }

      f(j)=math.sqrt(s)/coor(j)
      //println(f(j))
      f(j)=Math.atan(f(j)).toDegrees
    }

  }
  suma()
  angles()
  //println("lp "+sum+" f "+f(0)+" "+coor(0)+"\t"+coor(1))

}
