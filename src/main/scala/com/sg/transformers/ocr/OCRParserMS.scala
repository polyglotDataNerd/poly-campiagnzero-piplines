package com.sg.transformers.ocr

import java.io._

import com.crealytics.spark.excel.WorkbookReader
import com.sg.transformers.utility.SparkUtils
import com.sg.utils.ConfigProps
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.tika.Tika
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.{AutoDetectParser, ParseContext}
import org.apache.tika.sax.BodyContentHandler


class OCRParserMS extends java.io.Serializable {
  val config: ConfigProps = new ConfigProps()

  def parser(input: (String, PortableDataStream)): String = {

    val OCRparser: AutoDetectParser = new AutoDetectParser()
    val tika: Tika = new Tika()
    val stream: InputStream = input._2.open()
    val handler: BodyContentHandler = new BodyContentHandler()
    val metadata: Metadata = new Metadata()
    val context: ParseContext = new ParseContext()
    val excel = new StringBuilder()

    try {
      OCRparser.parse(stream, handler, metadata, context)
      stream.close()
    }
    catch {
      case exception: TikaException => println(exception.getMessage)
    }
    excel.append(input._1.replace("file:", "") + ": " + "Type formatted detected -> " + tika.detect(input._1.replace("file:", "")))
      .append(System.lineSeparator)
      .append(handler.toString.split("\n".trim).drop(1).mkString("\n".trim))
      .toString
  }


  def msDF(ss: SQLContext, sc: SparkContext, inPutPath: String, outPutPath: String, logstring: StringBuffer): Unit = {
    val sparkUtils: SparkUtils = new SparkUtils(sc, logstring)
    val sheetNames = WorkbookReader(Map("path" -> inPutPath)
      , sc.hadoopConfiguration
    ).sheetNames.toList

    sheetNames.foreach(x => {
      val fileName = x.replace(" ", "_").replace("-", "_")
      println(fileName)
      var df = ss
        .read
        .format("com.crealytics.spark.excel")
        .option("sheetName", x) //tab
        .option("useHeader", "false") /* Required: column headers aren't standardized and naming convention not best practice */
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true")
        .option("addColorColumns", "false")
        .load(inPutPath)
      val base = df.select(df.columns.map(x => regexp_replace(col(x), "(\r\n|\n\r|\r|\n)", " ").as(x.replace(" ", "_").toLowerCase())): _*)
      sparkUtils.gzipWriter(outPutPath + fileName + "/", base)
    })

  }


}
