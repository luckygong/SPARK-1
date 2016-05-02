import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating



object Repast extends App {
	val conf = new SparkConf().setAppName("Repast recommendation")
	val sc   = new SparkContext(conf)

	val data = sc.textFile("hdfs://localhost:9000/input_dir/repast/repast.data")

	val ratings = data.map(_.split('\t').take(3) match {case Array(user,item,rating) =>
			Rating(user.toInt,item.toInt,rating.toDouble)
		})


	ratings.cache()

	// 使用ASL构建推荐模型
	val rank  = 5
	val numIterations = 6
	val model = ALS.train(ratings,rank,numIterations,0.01)

	Utils.evaluteRepastM(ratings,model)

	//方便控制台查看
	println("rank(特征数目) ###########################################################")
	println(
	(20 to 30).map {r =>
		val model = ALS.train(ratings,r,numIterations,0.01)
		Utils.evaluteRepastM(ratings,model)
	}.mkString(" "))
	
	//方便控制台查看
	println("numIterations  ###########################################################")
	println(
	(20 to 30).map {n =>
		val model = ALS.train(ratings,n,numIterations,0.01)
		Utils.evaluteRepastM(ratings,model)
	}.mkString(" "))
	


	//推荐产品
	val userId = 789
	val K = 10
	val topKRecs = model.recommendProducts(userId, K)
}