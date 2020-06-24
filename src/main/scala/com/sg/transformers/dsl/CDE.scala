package com.sg.transformers.dsl

import java.net.URI
import com.sg.transformers.utility.SparkUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{when, _}


/* CRIME DATA EXPLORER DOMAIN SPECIFIC */
class CDE(sqlContext: SQLContext, sparkContext: SparkContext, endPoint: String, apiKey: String, outPutPath: String) {
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val date = format.format(new java.util.Date())
  private val stringBuffer: StringBuffer = new StringBuffer()
  private val sparkUtils: SparkUtils = new SparkUtils(sparkContext, stringBuffer)

  /*
  Agency Arrest Schema
  |-- category: string (nullable = true)
  |-- data: array (nullable = true)
  |    |-- element: struct (containsNull = true)
  |    |    |-- data_year: long (nullable = true)
  |    |    |-- key: string (nullable = true)
  |    |    |-- month_num: long (nullable = true)
  |    |    |-- value: long (nullable = true)
  |-- keys: array (nullable = true)
  |    |-- element: string (containsNull = true)
  |-- noun: string (nullable = true)
  |-- precise_data: array (nullable = true)
  |    |-- element: string (containsNull = true)
  |-- short_title: string (nullable = true)
  |-- title: string (nullable = true)
  |-- ui_restriction: string (nullable = true)
  |-- ui_type: string (nullable = true)
  */

  def getAgencyArrest(): Unit = {
    import sqlContext.implicits._
    /* get ORI codes */
    val ori = sqlContext
      .read
      .option("header", true)
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("s3a://poly-campaign-one/ori/ori_test.csv")
      .distinct()
      .collect()
      .map(_.toSeq)
      .flatten

    ori.foreach(oriKey => {
      val buildPath = endPoint + "arrest/agencies/offense/" + oriKey + "/all/1995/2018?API_KEY=" + apiKey
      sparkUtils.getAPIWithKeyRDD(buildPath).toMap.map(x => {
        /* checks API call to take in MAP with only 200 success calls, so API call will bypass any 500 errors */
        if (x._1.equals(200)) {
          println(x._1, " ", buildPath)
          val payloadDF =
            sqlContext
              .read
              .json(x._2.toDS)
              .select(
                lit(oriKey).as("ORI"),
                $"*",
                explode_outer(col("data")).as("data_array"),
                when(col("keys").isNotNull, concat_ws(",", col("keys"))).otherwise(lit(null)).as("key_array"),
                when(col("precise_data").isNotNull, concat_ws(",", col("precise_data"))).otherwise(lit(null)).as("precise_array")
              )
              .drop(col("data"))
              .drop(col("keys"))
              .drop(col("precise_data"))

          /* does a check to see if array is empty (NO DATA) is null skip, if not write to DataFrame */
          val nullcheck = payloadDF.select($"data_array").take(1).mkString(",").replaceAll("[\\[\\]]", "")
          println("nullcheck", nullcheck)
          if (!nullcheck.equals("null")) {
            val baseDF = payloadDF
              .select($"*", $"data_array.*")
              .drop(col("data_array"))

            /* rename spark output file */
            sparkUtils.gzipWriterAppend(outPutPath, baseDF)
            val fs = FileSystem.get(new URI(s"s3a://poly-campaign-one"), sparkContext.hadoopConfiguration)
            val fileName = fs.globStatus(new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests/" + date + "/part*"))(0).getPath.getName.trim
            fs.rename(new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests/" + date + "/" + fileName), new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests/" + date + "/" + oriKey + "_agency_arrests.gz"))
          }
        }
      })
    })

  }

  def getAgencyArrestOffense(): Unit = {
    import sqlContext.implicits._
    /* get ORI codes */
    val ori = sqlContext
      .read
      .option("header", true)
      .option("delimiter", "\t")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("s3a://poly-campaign-one/ori/ori_test.csv")
      .distinct()
      .collect()
      .map(_.toSeq)
      .flatten


    ori.foreach(oriKey => {
      val buildPath = endPoint + "arrest/agencies/" + oriKey + "/all/race/1995/2018?API_KEY=" + apiKey

      sparkUtils.getAPIWithKeyRDD(buildPath).toMap.map(x => {
        /* checks API call to take in MAP with only 200 success calls, so API call will bypass any 500 errors */
        if (x._1.equals(200)) {
          println(x._1, " ", buildPath)
          val payloadDF =
            sqlContext
              .read
              .json(x._2.toDS)
              .select(
                lit(oriKey).as("ORI"),
                $"*",
                explode_outer(col("data")).as("data_array"),
                when(col("keys").isNotNull, concat_ws(",", col("keys"))).otherwise(lit(null)).as("key_array"),
                when(col("precise_data").isNotNull, concat_ws(",", col("precise_data"))).otherwise(lit(null)).as("precise_array")
              )
              .drop(col("data"))
              .drop(col("keys"))
              .drop(col("precise_data"))

          /* does a check to see if array is empty (NO DATA) is null skip, if not write to DataFrame */
          val nullcheck = payloadDF.select($"data_array").take(1).mkString(",").replaceAll("[\\[\\]]", "")
          println("nullcheck", nullcheck)
          if (!nullcheck.equals("null")) {
            val baseDF = payloadDF
              .select($"*", $"data_array.*")
              .drop(col("data_array"))

            /* rename spark output file */
            sparkUtils.gzipWriterAppend(outPutPath, baseDF)
            val fs = FileSystem.get(new URI(s"s3a://poly-campaign-one"), sparkContext.hadoopConfiguration)
            val fileName = fs.globStatus(new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests_by_offense/" + date + "/part*"))(0).getPath.getName.trim
            fs.rename(new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests_by_offense/" + date + "/" + fileName), new Path("s3a://poly-campaign-one/crime_data_explorer/agency_arrests_by_offense/" + date + "/" + oriKey + "_agency_arrests_by_offense.gz"))
          }
        }
      })
    })
  }
}
