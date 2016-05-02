import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object Graph1 extends App {
	val hdfsServer = "hdfs://127.0.0.1:9000"
	val hdfsPath = "/input_dir"
	val vertexFile = hdfsServer + hdfsPath + "/graph1_vertex.csv"
	val edgeFile = hdfsServer + hdfsPath + "/graph1_edges.csv"	

	val sc = new SparkContext("local", "Graph1", "/home/linux/Applications/spark-1.3.1-bin-hadoop2.6", Nil, Map(), Map()) 

	val vertices:RDD[(VertexId,(String,String))] = sc.textFile(vertexFile).map{
		line => 
			val fields = line.split(",")
			(fields(0).toLong,(fields(1),fields(2)))
	}

	val edges:RDD[Edge[String]] = sc.textFile(edgeFile).map{
		line => 
			val fields = line.split(",")
			Edge(fields(0).toLong,fields(1).toLong,fields(2))
	}

	val default = ("Unknown","Missing")
	val graph = Graph(vertices,edges,default)

	println( "vertices : " + graph.vertices.count )
	println( "edges : " + graph.edges.count )
}

