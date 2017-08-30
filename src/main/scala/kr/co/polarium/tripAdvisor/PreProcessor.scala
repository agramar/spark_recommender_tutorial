package kr.co.polarium.tripAdvisor

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

import kr.co.polarium.tripAdvisor.model.Ratings

/**
 *  전처리기
 */
object PreProcessor {
  
  case class Ratings(userId: Long, hotelId: Long, rating: Double)

  case class HotelRating(userId: Int, hotelId: Int, rating: Float)
  
  // 원본 데이터 경로
 	val JSON_PATH = "file:///C:/Users/USER/Desktop/Private/SampleData/CF/TripAdvisorJson/json/729373.json"
 	val JSON_PATH_ALL = "file:///C:/Users/USER/Desktop/Private/SampleData/CF/TripAdvisorJson/json/*.json"
  
  // 스키마 정의(리플렉션 활용)
  val schema = ScalaReflection.schemaFor[Ratings].dataType.asInstanceOf[StructType]
  
  // 형변환(String to Number)
  val toInt = udf[Int, String](_.toInt)
  val toDouble = udf[Double, String](_.toDouble)
  val toFloat = udf[Float, String](_.toFloat)
  val toLong = udf[Long, String](_.toLong)
  
  def main (args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ALSExample")
      .getOrCreate()
    import spark.implicits._    
      
    val hotelReviews = spark.read.json(JSON_PATH)
//    hotelReviews.printSchema()
//    hotelReviews.show()

    // JSON 데이터 파싱
    val flattened = hotelReviews.select($"HotelInfo", explode($"Reviews").as("Reviews_flat"))
//    flattened.show()
    val ratingDF = flattened.select(($"HotelInfo.HotelID").as("hotelId"), ($"Reviews_flat.Author").as("userStringId"), ($"Reviews_flat.Ratings.Overall").as("rating"), ($"Reviews_flat.Date").as("timestamp"))
//    ratingDF.show()
    
    // 문자형식인 userid를 숫자형식으로 변환해서 맵핑
    val users = ratingDF
      .select(($"userStringId"))
      .dropDuplicates()
      .withColumn("userId", monotonically_increasing_id)
//    users.show()
        
    // 파싱한 평점 데이터에 생성한 숫자형식의 userid 조인
    val ratingsDF2 = ratingDF
      .join(users, ratingDF("userStringId") === users("userStringId"), "left")
      .withColumn("rating", toDouble(ratingDF("rating")))
      .withColumn("hotelId", toInt(ratingDF("hotelId")))
//    ratingsDF2.show()
//    ratingsDF2.printSchema()
    
    // 조인한 데이터에서 userId, hotelId, rating만 뽑아내기    
    val ratingsDF3 = ratingsDF2.select($"userId", $"hotelId", $"rating")
//    ratingsDF3.show()
    
    // Convert to DataSet from DataFrame
    val ratings = ratingsDF3.as[Ratings]
      .filter($"userId".isNotNull)
      .filter($"hotelId".isNotNull)
      .filter($"rating".isNotNull)
      
//    ratings.show()
    
    // 전처리한 데이터 CSV형식으로 저장
    ratings.coalesce(1).write.format("csv").save("data/tripAdvisor/ratings")
    users.coalesce(1).write.format("csv").save("data/tripAdvisor/users")
    
  }
  
}