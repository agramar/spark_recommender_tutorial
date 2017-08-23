package kr.co.polarium

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object WordCount {

  def main(args: Array[String]): Unit = {

    require(args.length == 3, "Usage: WordCount <Master> <Input> <Output>")

    // Step1: SparkContext 생성
    val sc = getSparkContext("WordCount", args(0))

    // Step2: 입력 소스로부터 RDD 생성
    val inputRDD = getInputRDD(sc, args(1))

    // Step3: 필요한 처리를 수행
    val resultRDD = process(inputRDD)

    // Step4: 수행 결과 처리
    handleResult(resultRDD, args(2))
  }

  def getSparkContext(appName: String, master: String) = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    new SparkContext(conf)
  }

  def getInputRDD(sc: SparkContext, input: String) = {
    sc.textFile(input)
  }

  def process(inputRDD: RDD[String]) = {
    val words = inputRDD.flatMap(str => str.split(" "))
    val wcPair = words.map((_, 1))
    wcPair.reduceByKey(_ + _)
  }

  def handleResult(resultRDD: RDD[(String, Int)], output: String) {
    resultRDD.saveAsTextFile(output);
  }
}