package ca.uwaterloo.ece657a.weather

import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._

class DailyConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(factor, city)
  val factor = opt[String](descr = "input path", required = true)
  val city = opt[String](descr = "city to aggregate temperature", required = true)
  verify()
}

object DailyData {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new DailyConf(argv)

    log.info("Number of reducers: " + args.city())

    val conf = new SparkConf().setAppName("HourlyTemperature")
    val sc = new SparkContext(conf)
    val col = cityMap.city(args.city()) + 1

    val outputDir = new Path("daily-" + args.factor() + "-" + args.city())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile("../historical-hourly-weather-data/" + args.factor() + ".csv")

    val zeroValues = (0f, 0) // sum, count
    val addValues = (aggValue: (Float, Int), iterValue: Float) => {
      var (sum, count) = aggValue
      val temp = iterValue
      sum += temp
      count += 1
      (sum, count)
    }
    val combineValues = (aggValue1: (Float, Int), aggValue2: (Float, Int)) =>
      (aggValue1._1 + aggValue2._1, aggValue1._2 + aggValue2._2)


    textFile
      .filter(line => {
        val tokens = line.split(",")
        tokens(col) != "" && !tokens(col).matches("[a-zA-Z]+")
      })
      .map(line => {
        val tokens = line.split(",")
        val day = tokens(0).take(10)
        val temp = tokens(col).toFloat - 273.15f
        (day, temp)
      })
      .aggregateByKey(zeroValues)(addValues, combineValues)
      .map(item => (item._1, item._2._1 / item._2._2))
      .repartition(1)
      .sortBy(_._1)
      .map(x => x._1 + "," + x._2)
      .saveAsTextFile("daily-" + args.factor() + "-" + args.city())
  }
}

