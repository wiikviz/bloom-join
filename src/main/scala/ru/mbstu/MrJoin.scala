package ru.mbstu

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MrJoin {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()

    val airportsRaw = spark.sparkContext.textFile("data/airports.csv")
    val airports: RDD[(String, (String, String, String, String))] = airportsRaw.map(line => {
      val tokens = line.split(",")
      (tokens(3), ("R", tokens(0), tokens(1), tokens(2))) // Tagging Locations with R
    })

    val flightDataRaw = spark.sparkContext.textFile("data/2008.csv")
    val flightData = flightDataRaw.map(line => {
      val tokens = line.split(",")
      (tokens(16), ("L", tokens(14), "", "")) // Tagging Locations with L
    })

    // spark.createDataFrame(flightData).show()
    println("flightData.count=" + flightData.count()) // 7009728

    val all = flightData union airports

    val grouped = all.groupByKey()

    val flightJoinAirports: RDD[(String, String)] = grouped.flatMap {
      case (key, iterable) =>
        // span returns two Iterable, one containing Flights and other containing Airports
        val (flight: Iterable[(String, String, String, String)], airport: Iterable[(String, String, String, String)]) = iterable span (_._1 == "L")
        airport.flatMap {
          case (_, name, _, _) =>
            flight.map(x => (name, x._2))
        }
    }

    //spark.createDataFrame(flightJoinAirports).show()

    flightJoinAirports.coalesce(1).saveAsTextFile("target/join-mr") // Saves output to the file.

    println("flightJoinAirports.count=" + flightJoinAirports.count())
    spark.stop()
  }
}