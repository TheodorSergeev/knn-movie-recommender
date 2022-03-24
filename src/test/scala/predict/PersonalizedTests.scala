package test.predict

import org.scalatest._
import funsuite._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import shared.predictions._
import tests.shared.helpers._
import ujson._

class PersonalizedTests extends AnyFunSuite with BeforeAndAfterAll {

  val separator = "\t"
  var spark : org.apache.spark.sql.SparkSession = _

  val train2Path = "data/ml-100k/u2.base"
  val test2Path = "data/ml-100k/u2.test"
  var train2 : Array[shared.predictions.Rating] = null
  var test2 : Array[shared.predictions.Rating] = null

  override def beforeAll {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    spark = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // For these questions, train and test are collected in a scala Array
    // to not depend on Spark
    train2 = load(spark, train2Path, separator).collect()
    test2 = load(spark, test2Path, separator).collect()
  }

  // All the functions definitions for the tests below (and the tests in other suites) 
  // should be in a single library, 'src/main/scala/shared/predictions.scala'.

  // Provide tests to show how to call your code to do the following tasks.
  // Ensure you use the same function calls to produce the JSON outputs in
  // src/main/scala/predict/Baseline.scala.
  // Add assertions with the answer you expect from your code, up to the 4th
  // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
  test("Test uniform unary similarities") { 
    // Create predictor with uniform similarities

    // Compute personalized prediction for user 1 on item 1
    val res_uniformPredUser1Item1 = personalizedRatingUniform(train2, 1, 1)
    assert(within(res_uniformPredUser1Item1, 4.0468, 0.0001))

    // MAE 
    def calcMAE = getFuncCalcMAE(train2, test2)
    val mae_OnesMAE = calcMAE(predictorPersonalizedUniform)
    assert(within(mae_OnesMAE, 0.7604, 0.0001))
  } 

  test("Test ajusted cosine similarity") { 
    // Create predictor with adjusted cosine similarities

    // Similarity between user 1 and user 2
    val res_AdjustedCosineUser1User2 = similarityCosineWithPreprocessing(1, 2, train2)
    assert(within(res_AdjustedCosineUser1User2, 0.0730, 0.0001))

    // Compute personalized prediction for user 1 on item 1
    val res_cosinePredUser1Item1 = personalizedRatingCosine(train2, 1, 1)
    assert(within(res_cosinePredUser1Item1, 4.0870, 0.0001))

    // MAE 
    def calcMAE = getFuncCalcMAE(train2, test2)
    val mae_AdjustedCosineMAE = calcMAE(predictorPersonalizedSimilarity(similarityCosine, _))
    assert(within(mae_AdjustedCosineMAE, 0.7372, 0.0001))
  }

  test("Test jaccard similarity") { 
    // Create predictor with jaccard similarities

    // Similarity between user 1 and user 2
    val res_JaccardUser1User2 = similarityJaccard(1, 2, train2)
    assert(within(res_JaccardUser1User2, 0.03187, 0.0001))

    // Compute personalized prediction for user 1 on item 1
    val res_jaccardPredUser1Item1 = personalizedRatingJaccard(train2, 1, 1)  
    assert(within(res_jaccardPredUser1Item1, 4.0982, 0.0001))

    // MAE 
    def calcMAE = getFuncCalcMAE(train2, test2)
    val mae_JaccardPersonalizedMAE = calcMAE(predictorPersonalizedSimilarity(similarityJaccard, _))
    assert(within(mae_JaccardPersonalizedMAE, 0.7556, 0.0001))
  }
}
