package com.sg.transformers.utility

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParametersRequest, GetParametersResult}
import com.google.auth.oauth2.GoogleCredentials
import com.sg.utils.Util
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class SparkUtils(sc: SparkContext, stringBuilder: java.lang.StringBuffer) extends java.io.Serializable {
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  private val date = format.format(new java.util.Date())
  private val partitions = Runtime.getRuntime.availableProcessors() * 9
  private val utils = new Util

  def orcWriter(target: String, df: DataFrame): Unit = {
    try {
      df
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("compression", "snappy")
        .option("orc.create.index", "true")
        .format("orc")
        .save(target)
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

  def gzipWriter(target: String, df: DataFrame): Unit = {
    try {
      df
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(target + date + "/")
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

  def gzipWriterAppend(target: String, df: DataFrame): Unit = {
    try {
      df
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .format("csv")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(target + date + "/")
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
      }
    }
  }

  def dbWrite(hostParam: String, uidParam: String, pwParam: String, tableName: String, dataFrame: DataFrame): Unit = {
    try {
      /*https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html*/
      dataFrame
        .repartition(Runtime.getRuntime.availableProcessors() * 2)
        .write
        .format("jdbc")
        .option("url", hostParam)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", tableName)
        .option("user", uidParam)
        .option("password", pwParam)
        .option("batchsize", 5000)
        .mode(SaveMode.Append)
        .save()
    }
    catch {
      case e: Exception => {
        println("Exception", e)
        stringBuilder.append("ERROR " + e.getMessage).append("\n")
        val trace = e.getStackTrace
        for (etrace <- trace) {
          println("Exception", etrace.toString)
          stringBuilder.append("ERROR " + etrace.toString).append("\n")
        }
      }
    }
  }

  def getSSMParam(param: String): String = {
    val cli = AWSSimpleSystemsManagementClientBuilder
      .standard
      .withRegion(Regions.US_WEST_2.getName)
      .withCredentials(new DefaultAWSCredentialsProviderChain)
      .build

    val request: GetParametersRequest = new GetParametersRequest
    request.withWithDecryption(true).withNames(param)
    val result: GetParametersResult = cli.getParameters(request)
    result.getParameters.get(0).getValue
  }

  def intFormatter(value: Any): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(value)
  }

  def googleAuth(bucket: String, credKeyPath: String): GoogleCredentials = {
    GoogleCredentials.fromStream(new Util().getS3Obj(bucket, credKeyPath))
  }


  def diffCols(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x
    match {
      case x if myCols.contains(x) => col(x.replaceAll("(\r\n|\n\r|\r|\n)", " "))
      case _ => lit("").as(x)
    }
    )
  }

  def getAPIContent(outputBucket: String, outPutKey: String, url: String): Unit = {
    var filename: String = new String
    val httpClient = HttpClientBuilder.create.build
    val get = new HttpGet(url)
    val response = httpClient.execute(get)

    if (response.containsHeader("Content-Disposition")) {
      response.getLastHeader("Content-Disposition").getElements.foreach(x => {
        filename = x.getParameterByName("filename").getValue
      })
    }
    else {
      filename = url.split("/").last.toLowerCase()
    }

    val is = response.getEntity.getContent
    utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + filename, is)
    if (response.getEntity.getContent != null) response.getEntity.getContent.close
    is.close()
    httpClient.close()
  }

  def getAPIWithKeyRDD(endPoint: String): scala.collection.mutable.HashMap[Int, RDD[String]] = {
    val httpClient = HttpClientBuilder.create.build
    val get = new HttpGet(endPoint)
    val response = httpClient.execute(get)
    var tgt = scala.collection.mutable.HashMap[Int, RDD[String]]()


    if (response.getStatusLine.getStatusCode.equals(200)) {
      val payload = IOUtils.toString(response.getEntity.getContent, "UTF-8")
      if (response.getEntity.getContent != null) response.getEntity.getContent.close
      tgt += (response.getStatusLine.getStatusCode -> sc.parallelize(Seq(payload)))
    }
    tgt
  }
}



