package com.sg.transformers.utility

import java.io.{InputStream, InputStreamReader}
import java.util.Collections
import java.util.UUID.randomUUID

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.{GetParametersRequest, GetParametersResult}
import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.client.googleapis.auth.oauth2._
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.sheets.v4.SheetsScopes
import com.sg.utils.Util
import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import scala.collection.JavaConverters._
import scala.collection.mutable;

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

  def googleAuth(bucket: String, credKeyPath: String): Credential = {
    /* https://developers.google.com/sheets/api/quickstart/java */
    val in: InputStream = new Util().getS3Obj(bucket, credKeyPath)
    val JSON_FACTORY = JacksonFactory.getDefaultInstance
    val HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport
    val SCOPES: java.util.List[String] = Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY);
    /* https://stackoverflow.com/questions/25900906/gmailapiquickstart */
    val gcs: GoogleClientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in))
    val flow: GoogleAuthorizationCodeFlow = new GoogleAuthorizationCodeFlow.Builder(
      HTTP_TRANSPORT, JSON_FACTORY, gcs, SCOPES)
      .setDataStoreFactory(new FileDataStoreFactory(new java.io.File("tokens")))
      .setAccessType("offline")
      .build()
    val receiver: LocalServerReceiver = new LocalServerReceiver.Builder().setPort(8888).build()
    new AuthorizationCodeInstalledApp(flow, receiver).authorize(null)
  }

  def googleService(bucket: String, credKeyPath: String): GoogleCredential = {
    GoogleCredential
      .fromStream(new Util().getS3Obj(bucket, credKeyPath))
      .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS_READONLY))
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
    val httpClient = HttpClientBuilder.create.build
    val get = new HttpGet(url)
    val response = httpClient.execute(get)
    var filename: String = new String

    /* text/csv type */
    if (response.getEntity.getContentType.getValue.contains("text/csv")) {

      if (response.containsHeader("Content-Disposition")) {
        filename = response.getLastHeader("Content-Disposition").getValue.split("=").last.replace("\"", "")
        val is = response.getEntity.getContent
        utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + filename, is)
        if (response.getEntity.getContent != null) response.getEntity.getContent.close
        is.close()
        httpClient.close()
      }
      else {
        filename = url.split("/").last.toLowerCase()
        val is = response.getEntity.getContent
        utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + filename, is)
        if (response.getEntity.getContent != null) response.getEntity.getContent.close
        is.close()
        httpClient.close()
      }

    }

    /* html type */
    else if (response.getEntity.getContentType.getValue.contains("text/html")) {
      val sb: StringBuffer = new StringBuffer()
      val quoteIdent = "\""
      val is = response.getEntity.getContent
      val htmlDoc: Document = Jsoup.parseBodyFragment(IOUtils.toString(response.getEntity.getContent, "UTF-8"))
      val rows: mutable.Buffer[Element] = htmlDoc.getElementsByTag("tr").asScala

      rows.foreach(r => {
        val cells: mutable.Buffer[Element] = r.getElementsByTag("td").asScala
        cells.foreach(c => {
          sb.append(quoteIdent).append(c.text()).append(quoteIdent).append(",")
        })
        /* removes last element of delimeter */
        if (sb.length() > 0) {
          sb.deleteCharAt(sb.length() - 1)
        }
        sb.append("\n")
      })

      utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + "html-" + randomUUID().toString, IOUtils.toInputStream(sb.toString, "UTF-8"))
      if (response.getEntity.getContent != null) response.getEntity.getContent.close
      is.close()
      httpClient.close()
    }

    /* excel/office docs */
    if (response.getEntity.getContentType.getValue.contains("officedocument")) {
      response.getLastHeader("Content-Disposition").getElements.foreach(x => {
        filename = x.getParameterByName("filename").getValue
        val is = response.getEntity.getContent
        utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + filename, is)
        if (response.getEntity.getContent != null) response.getEntity.getContent.close
        is.close()
        httpClient.close()
      })
    }

    /* compressed files */
    if (response.getEntity.getContentType.getValue.contains("application/binary")) {
      response.getLastHeader("Content-Disposition").getElements.foreach(x => {
        filename = x.getParameterByName("filename").getValue
        val is = response.getEntity.getContent
        utils.putS3Obj(outputBucket, outPutKey + "/" + date + "/" + filename, is)
        if (response.getEntity.getContent != null) response.getEntity.getContent.close
        is.close()
        httpClient.close()
      })
    }

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



