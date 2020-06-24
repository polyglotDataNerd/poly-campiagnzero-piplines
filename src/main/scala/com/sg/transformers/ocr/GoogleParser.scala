package com.sg.transformers.ocr


import java.io.File
import java.util.Collections

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

    /* temp copies P12 credential file from s3 */
    val tmpP12: File = File.createTempFile("p12", ".tmp")
    tmpP12.deleteOnExit();
    FileUtils.copyInputStreamToFile(new Util().getS3Obj("bigdata-utility", "google/development.p12"), tmpP12)
    println(FileUtils.readFileToString(tmpP12, "UTF-8"))

    val df = sqlContext
      .read
      .option("serviceAccountId", "development@development-123456789.iam.gserviceaccount.com")
      .option("credentialPath", tmpP12.getAbsolutePath)
      .option("multiline", "true")
      .format("com.github.potix2.spark.google.spreadsheets")
      .load(sheetPath)
      .persist(StorageLevel.MEMORY_ONLY_SER_2)
    /* removes all line breaks in all columns to be database compatible */
    val base = df.select(df.columns.map(x => regexp_replace(col(x), "(\r\n|\n\r|\r|\n)", " ").as(x.replace(" ", "_").toLowerCase())): _*)

    sparkUtils.gzipWriter(outPutPath, base)

  }

}
