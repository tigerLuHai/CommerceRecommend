package edu.cqu.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCT = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf配置
    val sparkConf: SparkConf = new SparkConf().setAppName("StatisticRecommender").setMaster(config("spark.cores"))
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //加入隐式转换
    import spark.implicits._   //用于将加载的数据转为Rating时
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //数据加载进来
    val ratingDF: DataFrame = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]  //中括号表示传入的必须是一个集合
      .toDF()
    //创建一张名叫ratings的表
    ratingDF.createOrReplaceTempView("ratings")

    //TODO:不同的统计推荐结果

    //1.统计所有历史数据中每个商品的评分数
    //数据结构 -》  productId,count
    val rateMoreProductsDF: DataFrame = spark.sql("select productId, count(productId) as count from ratings group by productId")
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCT)

    //2.统计以月为单位拟每个商品的评分数
    //数据结构 -》 productId,count,time
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //将原来的Rating数据集中的时间转换为年月格式
    val ratingOfYearMonth: DataFrame = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    //将新的数据集注册为一张新的表
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProducts: DataFrame = spark.sql("select productId, count(productId) as count,yearmonth from ratingOfMonth group by yearmonth ,productId order by yearmonth desc,count desc")
    storeDFInMongoDB(rateMoreRecentlyProducts,RATE_MORE_RECENTLY_PRODUCTS)

    //3.统计每个商品的平均评分
    val averageProductsDF: DataFrame = spark.sql("select productId,avg(score) as avg from ratings group by productId")
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)

    spark.stop()


  }
  def storeDFInMongoDB(df: DataFrame,collection_name: String)(implicit mongoConfig: MongoConfig):Unit = {
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
