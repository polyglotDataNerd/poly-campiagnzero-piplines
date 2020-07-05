package com.sg.transformers

import com.sg.utils.{ConfigProps, Util}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import com.sg.transformers.ocr.{GoogleParser, OCRParserMS, OCRParserPDF}
import com.sg.transformers.dsl._
import com.sg.transformers.utility._

/**
 * Created by gbartolome on 11/10/17.
 */
object OCREntry extends java.io.Serializable {

  private val config: ConfigProps = new ConfigProps
  private val utils: Util = new Util
  private val sb: StringBuffer = new StringBuffer()
  LogManager.getLogger("org").setLevel(Level.OFF)
  LogManager.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val parser: OCRParserPDF = new OCRParserPDF()
    val googleparser: GoogleParser = new GoogleParser()
    val msparser: OCRParserMS = new OCRParserMS()
    val runflag = args(0)

    /*local
    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkLocalMac")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.rpc.message.maxSize", 2047)
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .config("spark.hadoop.mapred.output.compress", "true")
      .config("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("spark.debug.maxToStringFields", 500)
      .config("spark.sql.caseSensitive", "false")
      .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .getOrCreate()*/

    /**/ val sparkSession = SparkSession
      .builder()
      .appName("spark-CampaignZero" + "-" + java.util.UUID.randomUUID())
      /* EMR 6.0.0 */
      .config("yarn.node-labels.enabled", "true")
      .config("yarn.node-labels.am.default-node-label-expression", "CORE")
      /* EMR 6.0.0 */
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", 2047)
      /* explicitly decreases shuffle */
      .config("spark.sql.shuffle.partitions", "10")
      .config("org.apache.spark.shuffle.sort.SortShuffleManager", "tungsten-sort")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .config("spark.sql.broadcastTimeout", "1600")
      .config("spark.network.timeout", "1600")
      .config("spark.debug.maxToStringFields", 1000)
      .config("spark.sql.orc.impl", "native")
      .config("spark.sql.orc.enableVectorizedReader", "true")
      .config("spark.sql.caseSensitive", "true")
      .config("spark.port.maxRetries", 256)
      .config("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.hadoop.fs.s3a.access.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/AccessKey"))
      .config("spark.hadoop.fs.s3a.secret.key", utils.getSSMParam("/s3/polyglotDataNerd/admin/SecretKey"))
      .enableHiveSupport()
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext
    sparkContext.hadoopConfiguration.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
    sparkContext.hadoopConfiguration.set("fs.s3a.enableServerSideEncryption", "true")
    sparkContext.hadoopConfiguration.set("fs.s3a.serverSideEncryptionAlgorithm", "AES256")

    /* PDF */
    if (runflag.toInt == 1) {
      parser.pdfDFText(sqlContext, sparkContext, args(1), args(2))
    }
    /* Google Docs */
    if (runflag.toInt == 2) {
      googleparser.sheetsParse(sqlContext, sparkContext, args(1), args(2), sb)
    }
    /* API Call No Auth */
    if (runflag.toInt == 3) {
      new SparkUtils(sparkContext, sb).getAPIContent(args(1), args(2), args(3))
    }
    /* Microsoft Excel Parser */
    if (runflag.toInt == 4) {
      msparser.msDF(sqlContext, sparkContext, args(1), args(2), sb)
    }
    /* DSL: CRIME DATA EXPLORER */
    if (runflag.toInt == 5) {
      /* By Demographic */
      new CDE(sqlContext, sparkContext, args(1), args(2), args(3)).getAgencyArrest()
    }
    if (runflag.toInt == 6) {
      /* By Demographic and Offense*/
      new CDE(sqlContext, sparkContext, args(1), args(2), args(3)).getAgencyArrestOffense()
    }
    /* DSL: CRIME DATA EXPLORER */

    sparkSession.stop()
  }


}
