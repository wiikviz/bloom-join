package ru.mbstu

import breeze.util.BloomFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MrJoinWithBloom {
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

    val airportCount = airports.count()
    val bf = airports.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](airportCount, 1-0.99)
      iter.foreach(i => bf += i._1)
      Iterator(bf)
    }.reduce(_ | _)

    val flightDataRaw = spark.sparkContext.textFile("data/2008.csv")
    val flightData = flightDataRaw.flatMap(line => {
      val tokens = line.split(",")
      val key = tokens(16)
      if (bf.contains(key))
        Seq((key, ("L", tokens(14), "", "")))// Tagging Locations with L)
      else
        Nil
    })

    // spark.createDataFrame(flightData).show()
    // MAGIC IS HERE
    println("flightData.count=" + flightData.count()) // 76031

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

    flightJoinAirports.coalesce(1).saveAsTextFile("target/join-mr-with-bloom") // Saves output to the file.

    println("flightJoinAirports.count=" + flightJoinAirports.count())
    spark.stop()
  }
}