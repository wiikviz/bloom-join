package ru.mbstu

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkJoin {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()

    val flightSchema = StructType(Array(
      StructField("year", IntegerType, true),                                                 //0
      StructField("month ", IntegerType, true),                                               //1
      StructField("day", IntegerType, true),                                                  //2
      StructField("day_of_week ", IntegerType, true),                                         //3
      StructField("dep_time", IntegerType, true),                                             //4
      StructField("crs_dep_time", IntegerType, true),                                         //5
      StructField("arr_time", IntegerType, true),                                             //6
      StructField("crs_arr_time", IntegerType, true),                                         //7
      StructField("unique_carrier", StringType, true),                                        //8
      StructField("flight_num", IntegerType, true),                                           //9
      StructField("tail_num", StringType, true),                                              //10
      StructField("actual_elapsed_time", IntegerType, true),                                  //11
      StructField("crs_elapsed_time", IntegerType, true),                                     //12
      StructField("air_time", IntegerType, true),                                             //13
      StructField("arr_delay", StringType, true),                                            //14
      StructField("dep_delay", IntegerType, true),                                            //15
      StructField("origin", StringType, true),                                                //16
      StructField("dest", StringType, true),                                                  //17
      StructField("distance", IntegerType, true),                                             //18
      StructField("taxi_in", IntegerType, true),                                              //19
      StructField("taxi_out", IntegerType, true),                                             //20
      StructField("cancelled", IntegerType, true),                                            //21
      StructField("cancellation_code", StringType, true),                                     //22
      StructField("diverted", IntegerType, true),                                             //23
      StructField("carrier_delay", StringType, true),                                         //24
      StructField("weather_delay", StringType, true),                                         //25
      StructField("nas_delay", StringType, true),                                             //26
      StructField("security_delay", StringType, true),                                        //27
      StructField("late_aircraft_delay", StringType, true),                                   //28
    )
    )

    val flightData = spark.read.format("csv")
      .option("delimiter",",")
      .option("quote","")
      .option("header", "false")
      .schema(flightSchema)
      .load("data/2008.csv")
      .toDF()

    flightData.createOrReplaceTempView("flight_data")

    val airportSchema = StructType(Array(
      StructField("name", StringType, true),                                               //0
      StructField("country", StringType, true),                                            //1
      StructField("area_code", IntegerType, true),                                         //1
      StructField("code", StringType, true),                                               //1
    )
    )

    val airportData = spark.read.format("csv")
      .option("delimiter",",")
      .option("quote","")
      .option("header", "false")
      .schema(airportSchema)
      .load("data/airports.csv")
      .toDF()

    airportData.createOrReplaceTempView("airports")

    println("flightData.count=" + flightData.count())

    val flightJoinAirports = spark.sql(
      """
        |SELECT
        |   name,
        |   arr_delay
        |FROM
        |   flight_data f
        |   INNER JOIN airports a
        |   ON (f.origin=a.code)
        |""".stripMargin)

    flightJoinAirports
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .csv("target/join-spark")

    println("flightJoinAirports.count=" + flightJoinAirports.count())

    spark.stop()
  }
}