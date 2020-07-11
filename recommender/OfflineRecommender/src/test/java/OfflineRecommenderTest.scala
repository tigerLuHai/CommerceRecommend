import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.junit._

class OfflineRecommenderTest {

  @Test
  def filterFind(): Unit ={
    val ratingExist: Array[Int] = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))("recommender")("Rating").find(MongoDBObject("userId" -> 4867)).toArray.map {
      item => item.get("productId").toString.toInt
    }
    print(ratingExist)
  }

}
