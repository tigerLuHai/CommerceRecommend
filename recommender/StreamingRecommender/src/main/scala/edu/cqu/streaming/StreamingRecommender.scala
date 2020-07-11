package edu.cqu.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import redis.clients.jedis.Jedis

object ConnHelper extends Serializable {
    lazy val jedis = new Jedis("localhost")
    lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri:String,db:String)
// 标准推荐
case class Recommendation(productId:Int, score:Double)
// 用户的推荐
case class UserRecs(userId:Int, recs:Seq[Recommendation])
//商品的相似度
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

object StreamingRecommender{
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个SparkSession and SparkStreamingContext
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext
    val streamingContext = new StreamingContext(sparkContext,Seconds(2))

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    val simProductMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map { recs =>
        (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)
      }.collectAsMap()
    val simProductsMatrixBroadCast: Broadcast[collection.Map[Int, Map[Int, Double]]] = sparkContext.broadcast(simProductMatrix)

    val kafkaConfig = Map(
      "bootstrap.servers" -> "192.168.1.21:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaConfig))
    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map {
      case msg =>
        val attr: Array[String] = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    //核心实时推荐算法
    ratingStream.foreachRDD{rdd =>rdd.map{
      case (userId,productId,score,timestamp) =>
        //获取当前最近的M次商品评分
        println(">>>>>>>>>>>>>>>>")
        val userRecentlyRatings: Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM,userId,ConnHelper.jedis)
      //获取商品P最相似的K个商品
      val simProducts: Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProductsMatrixBroadCast.value)
      //计算待选商品的推荐优先级
      computeProductScores(simProductsMatrixBroadCast.value,userRecentlyRatings,simProducts)
      //将数据保存到MongoDB

      //启动Streaming程序
      streamingContext.start()
        streamingContext.awaitTermination()
    }}
  }

  import scala.collection.JavaConversions._
  /**
    * 获取当前最近的M次评分商品
    * @param num 评分的个数
    * @param userId 评分用户
    * @param jedis
    * @return
    */
  def getUserRecentlyRating(num:Int, userId:Int,jedis:Jedis): Array[(Int,Double)] ={
    //从用户的队列中取出num个评分
    jedis.lrange("userId:" + userId.toString,0,num).map{
      item =>  val attr = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前商品k个相似的商品
    * @param num 相似商品的数量
    * @param productId 商品Id
    * @param userId 评分的用户
    * @param simproducts 商品相似度矩阵的广播变量值
    * @param mongoConfig MongoDB的配置
    * @return
    */
  def getTopSimProducts(num:Int,productId:Int,userId:Int,simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]])(implicit mongoConfig: MongoConfig):Array[Int] = {
    //从广播变量的相似度矩阵中获取当前商品所有的相似商品
    val allSimProducts: Array[(Int, Double)] = simProducts.get(productId).get.toArray
    //获取用户已经观看过的商品
    val ratingExist: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("userId" -> userId)).toArray.map {
      item => item.get("productId").toString.toInt
    }
    allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)
  }

  /**
    * 计算待选商品的推荐分数
    * @param simProducts  商品相似度矩阵
    * @param userRecentlyRatings  用户最近的k次评分
    * @param topSimproducts 当前商品最相似的k个商品
    * @return
    */
  def computeProductScores(simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],userRecentlyRatings:Array[(Int,Double)],topSimproducts:Array[Int]):Array[(Int,Double)] = {
    //用于保存每一个待选商品和最近评分的每一个商品的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    //用于保存每一个商品的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()
    //用于保存每一个商品的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()
    for (topSimProduct <- topSimproducts;userRecentlyRating <- userRecentlyRatings){
        val simScore: Double = getProductsSimScore(simProducts,userRecentlyRating._1,topSimProduct)
      if (simScore > 0.6){
        score += ((topSimProduct,simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3){
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct,0) + 1
        }else{
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct,0) + 1
        }
      }
    }
      score.groupBy(_._1).map{
        case (productId,sims) =>
          (productId,sims.map(_._2).sum / sims.length + log(increMap.getOrDefault(productId,1)) - log(decreMap.getOrDefault(productId,1)))
      }.toArray.sortWith(_._2 > _._2)
  }

  /**
    * 获取单个商品之间的相似度
    * @param simProducts 商品相似度矩阵
    * @param userRatingProduct 用户已经评分的商品
    * @param topSimProduct  候选商品
    * @return
    */
  def getProductsSimScore(simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]], userRatingProduct:Int, topSimProduct:Int): Double ={
    simProducts.get(topSimProduct) match {
      case Some(sim) => sim.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  def log(m:Int):Double = {
    math.log(m) / math.log(10)
  }

  /**
    * 将用户保存到mongoDB userId -> 1,recs-> 22:4.5|45:3.8
    * @param userId
    * @param streamRecs 流式推荐的结果
    * @param mongoConfig  MongoDBde配置
    */
  def saveRecsToMongoDB(userId:Int,streamRecs:Array[(Int,Double)])(implicit mongoConfig: MongoConfig):Unit = {
    //StreamRecs的连接
    val streaRecsCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId,
      "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1,"score"->x._2))))
  }
}
