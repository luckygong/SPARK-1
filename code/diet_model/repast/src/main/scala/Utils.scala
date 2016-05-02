import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel


object Utils{


	//评估Repast训练模型
	def evaluteRepastM(ratings: RDD[Rating],model:MatrixFactorizationModel):Double = {
		val userItems =ratings.map{case Rating(user,item,rating) =>
			(user,item)
		}

		val predictions = model.predict(userItems).map{case Rating(user,item,rating) =>
			((user,item),rating)
		}

		val ratesAndPreds = ratings.map{case Rating(user,item,rating) =>
			((user,item),rating)
		}.join(predictions)

		ratesAndPreds.map{case ((_,_),(r1,r2)) =>
			val err = (r1-r2)
			err * err
		}.mean()
	}
}