import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
  * Created by hdadmin on 12/5/16.
  */
object MovieRatings{
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Movie Ratings Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR");

    val userRatingsRDD = sc.textFile("file:///home/hdadmin/Downloads/ml-100k/u.data")
    val userRatingsTupleRDD = userRatingsRDD.map(line => (line.split("\\s+")(1), line.split("\\s+")(2).toFloat))
    //userRatingsTupleRDD.foreach(println)
    val groupedUserRatingsTupleRDD = userRatingsTupleRDD.groupByKey().mapValues(_.toList)
    //groupedUserRatingsTupleRDD.foreach(println)
    val averageRatingByMovieId = groupedUserRatingsTupleRDD.map(rec => (rec._1, average(rec._2)))
    //averageRatingByMovieId.foreach(println)

    val userItemRDD = sc.textFile("file:///home/hdadmin/Downloads/ml-100k/u.item")
    val userItemTupleRDD = userItemRDD.map(line => (line.split("\\|")(0), (line.split("\\|")(0), line.split("\\|")(1), line.split("\\|")(2), line.split("\\|")(4))))
    //userItemTupleRDD.foreach(println)

    val joinedRDD = userItemTupleRDD.join(averageRatingByMovieId)
    //joinedRDD.foreach(println)

    val finalResultRDD = joinedRDD.map(rec => rec._2._1._1 + "," + rec._2._1._2 + "," + rec._2._1._3+","+rec._2._1._4 + "," + rec._2._2)
    finalResultRDD.foreach(println)
  }
  def average[T](ts:List[T])( implicit num:Numeric[T])=
    num.toDouble(ts.sum) / ts.size
}


