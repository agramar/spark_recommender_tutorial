package kr.co.polarium.tripAdvisor

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import kr.co.polarium.tripAdvisor.model.HotelRating
import org.apache.spark.ml.recommendation.ALSModel



object ALSTrainer {

  val CSV_PATH = "data/result/tripAdvisor/ratings/*.csv"
  
  // ALS 모델 파라미터
  val MAX_ITER = 15
  val REG_PARAM = 0.01
  val RANK_PARAM = 10

  // 스키마 정의(리플렉션 활용)
  val schema = ScalaReflection.schemaFor[HotelRating].dataType.asInstanceOf[StructType]

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ALSTrainer")
      .getOrCreate()
    import spark.implicits._

    // 데이터 가져오기
    val ratings = spark.read
      .schema(schema)
      .csv(CSV_PATH)
      .as[HotelRating]
      .filter($"userId".isNotNull)
      .filter($"hotelId".isNotNull)
      .filter($"rating".isNotNull)
      
//    ratings.show()
      
    // 평점 최대값, 최저값
//    ratings.select(max('rating), mean('rating)).show

    // 카운트
//    val ratingCount = ratings.count()
//    println(s"USER 카운트 : $ratingCount")

    // 학습데이터를 이용한 ALS 추천 모델 생성
    // MaxIter : 반복 실행 횟수(기본값 10)
    // RegParam : ALS에서 사용할 정규화 파라미터(기본값 10)
    val als = new ALS()
      .setMaxIter(MAX_ITER)
      .setRegParam(REG_PARAM)
      .setRank(RANK_PARAM)
      .setUserCol("userId")
      .setItemCol("hotelId")
      .setRatingCol("rating")

    // 학습데이터와 검사데이터 8대 2 비율로 무작위 분리
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
      
    // 신규 모델 생성
    val model = als.fit(training)
 
    // 테스트 데이터로 RMSE를 계산해 모델 평가
    // cold start 전략을 drop 방식으로 설정했다는 염두에 두자(to ensure we don't get NaN evaluation metrics)
    // cold start : 
    // drop : 
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    // 평균 제곱근 편차(RMSE)
    // 추정 값 또는 모델이 예측한 값과 실제 환경에서 관찰되는 값의 차이를 다룰 때 흔히 사용하는 측도이다. 
    // 정밀도(precision)를 표현하는데 적합하다. 
    // 각각의 차이값은 잔차(residual)라고도 하며, 
    // 평균 제곱근 편차는 잔차들을 하나의 측도로 종합할 때 사용된다.
    val rmse = evaluator.evaluate(predictions)
    println(s"제곱근편차(Root Mean Square Error) : $rmse")
    
     // 모델 저장
//     model.write.overwrite().save("data/ALSModel/tripAdvisor")
     
    // 모델 불러오기
//    val model = ALSModel.load("data/ALSModel/tripAdvisor")

    // 각 사용자에게 top 10 영화추천을 생성
    //    val userRecs = model.recommendForAllUsers(10)

    // 각 영화에 top 10 사용자추천을 생성
    //    val movieRecs = model.recommendForAllItems(10)

    // 추천 결과 저장
    //    userRecs.write.format("json").save("data/tripAdvisor/result/userRecommends")

    // 결과 출력
    //    userRecs.show()
    //    movieRecs.show()

    spark.stop()
  }
}