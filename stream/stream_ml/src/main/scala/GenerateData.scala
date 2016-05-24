
import java.io.PrintWriter
import java.net.ServerSocket
import scala.util.Random


class GenerateData extends App{
  val MaxEvent = 100
  val NumFeature = 100
  val random = new Random()

  //生成稠密的正太分布的向量
  //Array.tabulate的作用
  //Returns an array containing values of a given function over a range of integer
  def generateRandomArray(n:Int) = Array.tabulate(n){_ =>
    random.nextGaussian()
  }


  val w = new breeze.linalg.DenseVector(generateRandomArray(NumFeature))
  val intercept = random.nextGaussian() * 10

  def generateNoisyData(n:Int) = {
    (1 to n) map { i=>
      val x = new breeze.linalg.DenseVector(generateRandomArray(NumFeature))
      val y = w.dot(x)
      val noisy:Double = y + intercept
      (noisy,x)
    }
  }

  val listener = new ServerSocket(9999)

  print("data source run in local[9999]...")

  while(true){
    val socket = listener.accept()
    new Thread(){
      override def run = {
        print("Got client connected from : " + socket.getInetAddress)
        val out = new PrintWriter(socket.getOutputStream(),true)
        while(true){
            Thread.sleep(1000)
            val num = random.nextInt(MaxEvent)
            val data = generateNoisyData(num)
           data.foreach{case (y,x) =>
               val xStr = x.data.mkString(",")
               val eventStr = s"$y\t$xStr"
               out.write(eventStr)
               out.write("\n")
           }
        }
      }
    }
  }
}
