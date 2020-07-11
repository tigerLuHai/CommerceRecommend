package edu.cqu.offline

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {


  def main(args: Array[String]): Unit = {
    val config = Map(
    "spark.cores" -> "local[*]",
    "mongo.uri" -> "mongodb://localhost:27017/recommender",
    "mongo.db" -> "recommender"
    )
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrain")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._
    //加载评分数据
    val ratingRDD: RDD[Rating] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection",OfflineRecommender.MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId, rating.productId, rating.score))
      .cache()
    //将一个RDD随机切分成两个RDD，用以切分训练集合测试集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8,0.2))

    val trainingRDD: RDD[Rating] = splits(0)
    val testingRDD: RDD[Rating] = splits(1)

    //输出最优参数
    adjustALSParams(trainingRDD,testingRDD)

    //关闭spark
    spark.close()
  }
  def adjustALSParams(trainingRDD: RDD[Rating], testingRDD: RDD[Rating]) = {
    //这里指定迭代次数为5，rank和lambda在几个值中选取调整
    val result = for(rank <- Array(100,200,500);lambda <- Array(1,0.1,0.01,0.001))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainingRDD,rank,5,lambda)
        val rmse: Double = getRMSE(model,testingRDD)
        (rank,lambda,rmse)
      }
    //println(result.sortBy(_._3).head)
  }

  def getRMSE(model: MatrixFactorizationModel, value: RDD[Rating]):Double = {
    val userProducts: RDD[(Int, Int)] = value.map(item => (item.user,item.product))
    val predictRating: RDD[Rating] = model.predict(userProducts)
    val real: RDD[((Int, Int), Double)] = value.map(item => ((item.user,item.product),item.rating))
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user,item.product),item.rating))
    //计算RMSE
    sqrt(real.join(predict).map{
      case((userId,productId),(real,pre)) => val err = real - pre
        err * err
    }.mean())
  }
}
