import scala.io.Source
import scala.collection.mutable.ListBuffer
object Word_Counter {

  def main(args: Array[String]): Unit =
  {
    val f: Source = Source.fromFile("/home/fieldemployee/Downloads/Example.txt")
    var l: ListBuffer[String] = ListBuffer()
    var y: ListBuffer[Integer] = ListBuffer()
    var x: ListBuffer[String] = ListBuffer()
    var i: Int = 0
    for (line <- f.getLines()) {
      l += line
    }
    var truelist: List[String] = l.toList
    for (x <- 0 to truelist.length -1)
      {
        var nList:Array[String] = l(x).split(" ")
        for (y <- 0 to nList.length -1)
          {
            var n:String = nList(y)
            if (x.contains(n))
            {
              var index:Int = x.indexOf(n)
              y(index) = y(index) + 1

            }
            else
            {
              x += n
              y += 1
            }
          }
      }

    for (j <- 0 to y.length - 1)
      {
        print (word(j) +" : "+ y(j) + " ")
      }
    f.close()
  }
}
