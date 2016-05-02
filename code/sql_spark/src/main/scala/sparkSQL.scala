import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructType, StructField, StringType}


object sparkSQL extends App {
    val appName = "sql example 1"
    val conf = new SparkConf()

    conf.setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //导入隐式转换
    import sqlContext.implicits._
    //读取数据
    val rawRdd = sc.textFile("hdfs://127.0.0.1:9000/input_dir/rddText.txt")
    //创建schema表
    val schemaString = "age workclass fnlwgt"
    val schema = StructType(schemaString.split(" ").map(filedName =>
        StructField(filedName,StringType,true)))

    val rowRdd = rawRdd.map(_.split(",")).map(p =>
        Row(p(0),p(1),p(2)))

    //创建DataFrame
    val adultDataFrame = sqlContext.createDataFrame(rowRdd,schema)

    //1.转换为JSON格式
    val jsonData = adultDataFrame.toJSON

    //2.保存JSON
//    jsonData.saveAsTextFile("people.json")

    println(jsonData.first.toString())

    //3.保存parquet
//    adultDataFrame. write.parquet("people.parquet")

    //4.操作
    //(1) 第一种方法
    adultDataFrame.select("age","fnlwgt").filter("age > 21").show()

    adultDataFrame.select("age","fnlwgt")
            .groupBy("age")
            .count()
            .sort("age")
            .show()

    // (2) 使用SQL操作
    adultDataFrame.registerTempTable("adult")
    sqlContext.sql("select age,count(age) from adult group by age").collect().foreach(println)
    // 我们之前的schema的字段类型都是String,为了能使用where,我们得指定具体的类型
    val schema2 =
        StructType(
              StructField("age", IntegerType, false) ::
              StructField("workclass", StringType, false) ::
              StructField("fnlwgt", IntegerType, false) :: Nil
        )
    val rowRdd2 = rawRdd.map(_.split(",")).map(p =>
        Row(p(0).toInt,p(1),p(2).toInt))

    val adultDataFrame2 = sqlContext.createDataFrame(rowRdd2,schema2)
    adultDataFrame2.registerTempTable("adult2")

    sqlContext.sql("SELECT * FROM adult WHERE age < 60").show()
    //JOIN 操作,当JOIN的一方有多个 == 时,例如:a1.fnlwgt = a2.fnlwgt中多个a2.fnlwgt相同,那么a1.fnlwgt会拷贝一份相同的来对应
    sqlContext.sql("SELECT * FROM " +
      "(SELECT age as age1,fnlwgt from adult where age > 20) a1 JOIN (SELECT age as age2,fnlwgt from adult where age > 20) a2 " +
      "on (a1.fnlwgt = a2.fnlwgt) LIMIT 10")
      .show()

    //5.自定义函数

    def checkNum(fnlwgt:Int):Int = fnlwgt match{
        case  3400  =>  5000
        case  9400  =>  6000
        case  i     =>  i
    }

    //注册函数
    sqlContext.udf.register("checkNum",checkNum _)
    sqlContext.sql("select age,workclass,checkNum(fnlwgt) from adult2").collect().foreach(println)

}
