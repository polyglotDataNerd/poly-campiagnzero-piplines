package com.sg.transformers.ocr


import java.io.File
import scala.collection.JavaConversions._

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.sheets.v4.Sheets
import com.google.api.services.sheets.v4.model.Spreadsheet
import com.sg.transformers.utility.SparkUtils
import com.sg.utils.Util
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

class GoogleParser extends java.io.Serializable {

  def sheetsParse(sqlContext: SQLContext, sparkContext: SparkContext, sheetPath: String, outPutPath: String, logstring: StringBuffer): Unit = {
    val sparkUtils: SparkUtils = new SparkUtils(sparkContext, logstring)
    val credential = sparkUtils.googleAuth("bigdata-utility", "google/oauth.json")

    /* temp copies P12 credential file from s3 */
    val tmpP12: File = File.createTempFile("p12", ".tmp")
    tmpP12.deleteOnExit();
    FileUtils.copyInputStreamToFile(new Util().getS3Obj("bigdata-utility", "google/development.p12"), tmpP12)
    val parent: Sheets = new Sheets
    .Builder(GoogleNetHttpTransport.newTrustedTransport(), JacksonFactory.getDefaultInstance, credential)
      .setApplicationName("test").build()
    val sheet: Spreadsheet = parent.spreadsheets().get(sheetPath).setIncludeGridData(false).execute()

    for (s <- sheet.getSheets) {
      val loadSheet = sheetPath + "/" + s.getProperties.getTitle
      println(loadSheet)
      /* excludes currently editing sheets */
      if (
        !s.getProperties.getTitle.contains("(in Progress)")
      ) {
        val df = sqlContext
          .read
          .option("serviceAccountId", "development@development-281015.iam.gserviceaccount.com")
          .option("credentialPath", tmpP12.getAbsolutePath)
          .option("header", "false")
          .option("multiline", "true")
          .format("com.github.potix2.spark.google.spreadsheets")
          .load(loadSheet)
          .persist(StorageLevel.MEMORY_ONLY_SER_2)
        /* removes all line breaks in all columns to be database compatible */
        val base = df.select(df.columns.map(x => regexp_replace(col(x), "(\r\n|\n\r|\r|\n)", " ").as(x.replace(" ", "_").toLowerCase())): _*)
        sparkUtils.gzipWriter(
          outPutPath + "/" +
            s.getProperties.getTitle.replace(" ", "_").toLowerCase().trim + "/"
          , base)
      }
    }
  }


}
