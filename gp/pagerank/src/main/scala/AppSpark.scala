import org.apache.spark._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.graphx._


class AppSpark extends App {
  val interval = args(0).toInt
  val conf = new SparkConf().setAppName("Master")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  case class User(name: String, age: Int)

  val users = List((1L, User("Alex", 26)), (2L, User("Bill", 42)), (3L, User("Carol", 18)),
    (4L, User("Dave", 16)), (5L, User("Eve", 45)), (6L, User("Farell", 30)),
    (7L, User("Garry", 32)), (8L, User("Harry", 36)), (9L, User("Ivan", 28)),
    (10L, User("Jill", 48)))

  val follows = List(Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 1L, 1), Edge(3L, 4L, 1),
    Edge(3L, 5L, 1), Edge(4L, 5L, 1), Edge(6L, 5L, 1), Edge(7L, 6L, 1),
    Edge(6L, 8L, 1), Edge(7L, 8L, 1), Edge(7L, 9L, 1), Edge(9L, 8L, 1),
    Edge(8L, 10L, 1), Edge(10L, 9L, 1), Edge(1L, 11L, 1))


  val usersRDD = sc.parallelize(users)
  val followsRDD = sc.parallelize(follows)

  val defaultUser = User("NA", 0)
  val socialGraph = Graph(usersRDD, followsRDD, defaultUser)

  /*
  def pregel[A: ClassTag](
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)(
      vprog: (VertexId, VD, A) => VD,//处理消息 VD:节点属性,A:消息
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] = {
    Pregel(graph, initialMsg, maxIterations, activeDirection)(vprog, sendMsg, mergeMsg)
  }
   */
  //**********************计算顶点的出度数A,并更新edge的值为1/A以及更新顶点为1
  val outDegrees = socialGraph.outDegrees

  val outDegreesGraph = socialGraph.outerJoinVertices(outDegrees) {
    (vId, vData, OptOutDegree) =>
      //更新顶点数据为出度
      OptOutDegree.getOrElse(0)
  }


  val weightedEdgesGraph = outDegreesGraph.mapTriplets { EdgeTriplet =>
    //边界的值
    //1/出度数目
    1.0 / EdgeTriplet.srcAttr
  }

  //重新把顶点数据设置为一
  val inputGraph = weightedEdgesGraph.mapVertices((id, vData) => 1.0)
  //**********************

  //**********************计算pageRanks**********************
  //1.所有顶点的消息初始化为0
  //2.遍历每个triplet,把出度数发射给目标对象
  //3.目标合并所有的出度数(每次都利用上次的Iterator)
  //4.updateVertex 最终处理
  val firstMessage = 0.0
  val iterations = 20
  val edgeDirection = EdgeDirection.Out

  val updateVertex = (vId: Long, vData: Double, msgSum: Double) => 0.15 + 0.85 * msgSum

  //triplet.srcAttr = 1
  val sendMsg = (triplet: EdgeTriplet[Double, Double]) => Iterator((triplet.dstId, triplet.srcAttr * triplet.attr))

  val aggregateMsgs = (x: Double, y: Double) => x + y

  //1.sendMsg
  //2.aggregateMsgs
  //3.updateVertex
  val influenceGraph = inputGraph.pregel(firstMessage, iterations, edgeDirection)(updateVertex, sendMsg, aggregateMsgs)

  //**********************打印数据**********************
  val userNames = socialGraph.mapVertices { (vId, vData) => vData.name }.vertices
  val userNamesAndRanks = influenceGraph.outerJoinVertices(userNames) {
    (vId, rank, optUserName) =>
      (optUserName.get, rank)
  }.vertices
  userNamesAndRanks.collect.foreach { case (vId, vData) =>
    println(vData._1 + "'s influence rank: " + vData._2)
  }
}
