import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations._
import org.apache.spark.streaming.dstream.DStream

object StreamingModel extends App {

  val ssc = new StreamingContext("local[2]", "stream model train...", seconds(10))
  val stream = ssc.socketTextStream("localhost", 9999)
  val NumFeatures = 100
  val zeroVector = DenseVector.zeros[Double](NumFeatures)
  val model = new StreamingLinearRegressionWithSGD()
    .setNumIterations(1)
    .setInitialWeights(Vectors.dense(zeroVector.data))
    .setStepSize(0.01)

  val labelStream:DStream[LabeledPoint] = stream.map{event =>
    val split = event.split("\t")
    val y = split(0).toDouble
    val features = split(1).split(",").map(_.toDouble)
    LabeledPoint(label = y,features = Vectors.dense(features))
  }

  //def trainOn(data: DStream[LabeledPoint]): Unit
  model.trainOn(labelStream)
  //def predictOn(data: DStream[Vector]): DStream[Double]
  model.predictOn(labelStream.map(_.features))

  ssc.start()
  //等待终止
  ssc.awaitTermination()
}
