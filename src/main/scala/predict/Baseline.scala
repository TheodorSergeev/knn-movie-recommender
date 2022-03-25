// runMain predict.Baseline --train data/ml-100k/u2.base --test data/ml-100k/u2.test --json baseline-100k.json --num_measurements 3
// runMain predict.Baseline --train data/ml-25m/r2.train --test data/ml-25m/r2.test --separator , --json baseline-25m.json  --num_measurements 3
package predict

import org.rogach.scallop._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.math
import shared.predictions._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val separator = opt[String](default=Some("\t"))
  val num_measurements = opt[Int](default=Some(0))
  val json = opt[String]()
  verify()
}

object Baseline extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args)

  // for these questions, data is collected in a scala Array to not depend on Spark
  println("Loading training data from: " + conf.train()) 
  val train = load(spark, conf.train(), conf.separator()).collect()
  println("Loading test data from: " + conf.test()) 
  val test = load(spark, conf.test(), conf.separator()).collect()


  /*
  Linux-specific commands for system specs
    model                       : ?
    CPU speed                   : lscpu | grep MHz
    RAM                         : grep MemTotal /proc/meminfo
    OS                          : hostnamectl
    versions of JVM, Scala, sbt : sbt scalaVersion
  */

  /*
  Windows-specific commands for system specs
    CPU speed | Get-WmiObject Win32_Processor
    OS RAM    | systeminfo
    JVM       | About Java application
    sbt       | sbt sbtVersion
    scala     | sbt scalaVersion
  */

  // calculate MAE on the test dataset for a predictor trained on a train dataset
  def calcMAE = getFuncCalcMAE(train, test)
  def calcMAETimings = getFuncCalcMAETimings(train, test, conf.num_measurements())

  // runs ~250 sec

  println("Computing single predictions")
  val res_GlobalAvg      = globalAvgRating(train)
  val res_User1Avg       = userAvgRating  (train, 1)
  val res_Item1Avg       = itemAvgRating  (train, 1)
  val res_Item1AvgDev    = itemAvgDev     (train, 1)
  val res_PredUser1Item1 = baselineRating (train, 1, 1)

  println("Computing MAE")
  val mae_GlobalAvg = calcMAE(predictorGlobalAvg)
  val mae_UserAvg   = calcMAE(predictorUserAvg)
  val mae_ItemAvg   = calcMAE(predictorItemAvg)
  val mae_Baseline  = calcMAE(predictorBaseline)

  println("Computing timings")
  val t_globalAvg = calcMAETimings(predictorGlobalAvg)
  val t_userAvg   = calcMAETimings(predictorUserAvg)
  val t_itemAvg   = calcMAETimings(predictorItemAvg) 
  val t_baseline  = calcMAETimings(predictorBaseline)

  println("Saving results")


  // Save answers as JSON
  def printToFile(content: String, location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var answers = ujson.Obj(
        "Meta" -> ujson.Obj(
          "1.Train" -> ujson.Str(conf.train()),
          "2.Test" -> ujson.Str(conf.test()),
          "3.Measurements" -> ujson.Num(conf.num_measurements())
        ),
        "B.1" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Num(res_GlobalAvg),          // Datatype of answer: Double
          "2.User1Avg" -> ujson.Num(res_User1Avg),            // Datatype of answer: Double
          "3.Item1Avg" -> ujson.Num(res_Item1Avg),            // Datatype of answer: Double
          "4.Item1AvgDev" -> ujson.Num(res_Item1AvgDev),      // Datatype of answer: Double
          "5.PredUser1Item1" -> ujson.Num(res_PredUser1Item1) // Datatype of answer: Double
        ),
        "B.2" -> ujson.Obj(
          "1.GlobalAvgMAE" -> ujson.Num(mae_GlobalAvg),       // Datatype of answer: Double
          "2.UserAvgMAE" -> ujson.Num(mae_UserAvg),           // Datatype of answer: Double
          "3.ItemAvgMAE" -> ujson.Num(mae_ItemAvg),           // Datatype of answer: Double
          "4.BaselineMAE" -> ujson.Num(mae_Baseline)          // Datatype of answer: Double
        ),
        "B.3" -> ujson.Obj(
          "1.GlobalAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(t_globalAvg)),   // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(t_globalAvg))      // Datatype of answer: Double
          ),
          "2.UserAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(t_userAvg)),     // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(t_userAvg))        // Datatype of answer: Double
          ),
          "3.ItemAvg" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(t_itemAvg)),     // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(t_itemAvg))        // Datatype of answer: Double
          ),
          "4.Baseline" -> ujson.Obj(
            "average (ms)" -> ujson.Num(mean(t_baseline)),    // Datatype of answer: Double
            "stddev (ms)" -> ujson.Num(std(t_baseline))       // Datatype of answer: Double
          )
        )
      )

      val json = ujson.write(answers, 4)
      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json.toString, jsonFile)
    }
  }

  println("")
  spark.close()
}
