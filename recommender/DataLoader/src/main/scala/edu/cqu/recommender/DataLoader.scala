package edu.cqu.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Product(productId: Int,name: String,imageUrl: String,categories: String,tags:String)

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

object DataLoader {

  val PRODUCT_DATA_PATH = "D:\\IntellijCode\\CommerceRecommend\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\IntellijCode\\CommerceRecommend\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    //定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建一个SparkConf配置
    val sparkConf: SparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    //创建一个SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //在对DataFrame和DataSet进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    //将Product、Rating数据集加载进来
    val productRDD: RDD[String] = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF: DataFrame = productRDD.map(item => {
        val attr: Array[String] = item.split("\\^")
        Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(RATING_DATA_PATH)
    //将ratingRDD转换为DataFrame
    val ratingDF: DataFrame = ratingRDD.map(item => {
      val attr: Array[String] = item.split((","))
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    //声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)
    //将数据保存到MongoDB中
    storeDataInMongoDB(productDF,ratingDF)

    //关闭Spark
    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建一个到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //定义通过MongoDB客户端拿到的表操作对象
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //如果MongoDB中有对应的数据库，那么应该删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据写入到MongoDB
    productDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //对数据表建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))  //1表示升序
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    //关闭MongoDB的连接
    mongoClient.close()
  }
}
