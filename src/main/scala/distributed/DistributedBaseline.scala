package distributed

import org.rogach.scallop._
import org.apache.spark.rdd.RDD
import ujson._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val master = opt[String](default=Some(""))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object DistributedBaseline extends App {
  var conf = new Conf(args) 

  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = if (conf.master() != "") {
    SparkSession.builder().master(conf.master()).getOrCreate()
  } else {
    SparkSession.builder().getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator())
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator())


  // get the number of executors from the command line parameters
  /*val pattern = """local\[([0-9]+)\]""".r
  val pattern(num_exec_str) = conf.master()
  val num_exec = num_exec_str.toInt*/

  // calculate MAE on the test dataset for a predictor trained on a train dataset
  def calcMAE = distr_getFuncCalcMAE(train, test)
  def calcMAETimings = distr_getFuncCalcMAETimings(train, test, conf.num_measurements())

  // runs ~??? sec

  println("Computing single predictions")
  val res_GlobalAvg      = distr_globalAvgRating(train)
  val res_User1GlobalAvg = distr_userAvgRating  (train, 1)
  val res_Item1GlobalAvg = distr_itemAvgRating  (train, 1)
  val res_Item1AvgDev    = distr_itemAvgDev     (train, 1)
  val res_PredUser1Item1 = distr_baselineRating (train, 1, 1)

  println("Computing MAE")
  val res_Mae = calcMAE(distr_predictorBaseline)

  println("Computing timings")
  val t_distr  = calcMAETimings(distr_predictorBaseline)


  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      val answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> conf.train(),
          "2.Test" -> conf.test(),
          "3.Master" -> conf.master(),
          "4.Measurements" -> conf.num_measurements()
        ),
        "D.1" -> ujson.Obj(
          "1.GlobalAvg"      -> ujson.Num(res_GlobalAvg),      // Datatype of answer: Double
          "2.User1Avg"       -> ujson.Num(res_User1GlobalAvg), // Datatype of answer: Double
          "3.Item1Avg"       -> ujson.Num(res_Item1GlobalAvg), // Datatype of answer: Double
          "4.Item1AvgDev"    -> ujson.Num(res_Item1AvgDev),    // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(res_PredUser1Item1), // Datatype of answer: Double
          "6.Mae"            -> ujson.Num(res_Mae)             // Datatype of answer: Double
        ),
        "D.2" -> ujson.Obj(
          "1.DistributedBaseline" -> ujson.Obj(
            "average (ms)"   -> ujson.Num(mean(t_distr)),      // Datatype of answer: Double
            "stddev (ms)"    -> ujson.Num(std (t_distr))       // Datatype of answer: Double
          )
        )
      )
      val json = write(answers, 4)

      println(jsonFile)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
