package ca.uwaterloo.ece657a.weather

import org.apache.hadoop.fs._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._

class PredictConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(city, accurate)
  val city = opt[String](descr = "city to aggregate temperature", required = true)
  val accurate = toggle("accurate")
  verify()
}

object PredictData {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PredictConf(argv)

    log.info("Number of reducers: " + args.city())

    val conf = new SparkConf().setAppName("HourlyTemperature")
    val sc = new SparkContext(conf)
    val col = cityMap.city(args.city()) + 1
    val detailed = args.accurate.isSupplied

    def mapToCity(r: RDD[String], changeTemperature: Boolean = false) = {
      r.flatMap(line => {
        val tokens = line.split(",")
        if (tokens(col) != "") {
          if (changeTemperature && !tokens(col).matches("[a-zA-Z\\s]+")) {
            val timestamp = tokens(0)
            val value = (tokens(col).toFloat - 273.15).toString
            List((timestamp, value))
          } else {
            val timestamp = tokens(0)
            val value = tokens(col)
            List((timestamp, value))
          }
        } else {
          List()
        }
      })
    }

    val outputDir = new Path("predict-" + args.city())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val temperature = mapToCity(sc.textFile("../historical-hourly-weather-data/temperature.csv"), changeTemperature = true)
    val humidity = mapToCity(sc.textFile("../historical-hourly-weather-data/humidity.csv"))
    val pressure = mapToCity(sc.textFile("../historical-hourly-weather-data/pressure.csv"))
    val wind = mapToCity(sc.textFile("../historical-hourly-weather-data/wind_speed.csv"))
    val description = mapToCity(sc.textFile("../historical-hourly-weather-data/weather_description.csv"))

    temperature
      .join(humidity)
      .join(pressure)
      .join(wind)
      .join(description)
      .repartition(1)
      .map(l => {
        val day = l._1.take(10)
        val hour = l._1.slice(11, 13)
        if (detailed) (day, hour, l._2._1._1._1._1, l._2._1._1._1._2, l._2._1._1._2, l._2._1._2, l._2._2)
        else (day, hour, l._2._1._1._1._1, l._2._1._1._1._2, l._2._1._1._2, l._2._1._2, l._2._2.split(" ").takeRight(1)(0))
      })
      .sortBy(_._1)
      .map(l => l._1 + "," + l._2 + "," + l._3 + "," + l._4 + "," + l._5 + "," + l._6 + "," + l._7)
      .saveAsTextFile("predict-" + args.city())
  }
}

