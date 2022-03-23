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

  val res_GlobalAvg      = globalAvgRating(train)
  val res_User1Avg       = userAvgRating  (train, 1)
  val res_Item1Avg       = itemAvgRating  (train, 1)
  val res_Item1AvgDev    = itemAvgDev     (train, 1)
  val res_PredUser1Item1 = baselineRating (train, 1, 1)

  val mae_GlobalAvg = globalAvgRatingMAE(train, test)
  val mae_UserAvg   = userAvgRatingMAE  (train, test)
  val mae_ItemAvg   = itemAvgRatingMAE  (train, test)
  val mae_Baseline  = baselineRatingMAE (train, test)

  val num_runs = conf.num_measurements() // 3
  val t_globalAvg = getTimings(globalAvgRatingMAE, train, test, num_runs)
  val t_userAvg   = getTimings(  userAvgRatingMAE, train, test, num_runs)
  val t_itemAvg   = getTimings(  itemAvgRatingMAE, train, test, num_runs)
  val t_baseline  = getTimings( baselineRatingMAE, train, test, num_runs)

  /*
  Linux-specific commands
    model                       : ?
    CPU speed                   : lscpu | grep MHz
    RAM                         : grep MemTotal /proc/meminfo
    OS                          : hostnamectl
    versions of JVM, Scala, sbt : sbt scalaVersion
  */

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
