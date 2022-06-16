import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main extends App {
  println("Hello, World!")

  // 这里的下划线"_"是占位符，代表数据文件的根目录
  val rootPath: String = "."
  val file: String = s"$rootPath/wikiOfSpark.txt"

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Spark SQL basic example")
  var spark: SparkSession = _
  try {
    spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 读取文件内容
    val lineRDD: RDD[String] = spark.sparkContext.textFile(file)

    lineRDD.take(10).foreach(println)

    // 以行为单位做分词
    val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))
    wordRDD.take(200).foreach(println)

    // 过滤掉空字符串
    val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))
    cleanWordRDD.take(200).foreach(println)

    // 把RDD元素转换为（Key，Value）的形式
    val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
    kvRDD.take(10).foreach(println)

    // 按照单词做分组计数
    val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
    wordCounts.take(10).foreach(println)

    // 打印词频最高的5个词汇
    val top5: Array[(Int, String)] = wordCounts.map { case (k, v) => (v, k) }.sortByKey(ascending = false).take(5)

    println(top5.mkString("Array(", ", ", ")"))

  } finally {
    if (spark != null) {
      spark.close()
    }
  }

}