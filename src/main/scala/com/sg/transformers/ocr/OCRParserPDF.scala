package com.sg.transformers.ocr

import java.io._

import com.sg.transformers.utility.SparkUtils
import com.sg.utils.ConfigProps
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.sax.BodyContentHandler


class OCRParserPDF extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps()
  private val format = new java.text.SimpleDateFormat("yyyy-MM-dd")


  def parser(input: (String, PortableDataStream)): String = {
    val OCRparser: PDFParser = new PDFParser()
    val stream: InputStream = input._2.open()
    val stringWrite: StringWriter = new StringWriter()
    val handler: BodyContentHandler = new BodyContentHandler(stringWrite)
    val metadata: Metadata = new Metadata()
    val context: ParseContext = new ParseContext()
    val pdf = new StringBuffer()
    try {
      OCRparser.parse(stream, handler, metadata, context)
    }
    catch {
      case exception: TikaException => println(exception.getMessage)
    }
    finally {
      stream.close()
      stringWrite.close()
    }
    pdf
      .append(handler.toString)
      .toString()
  }

  def pdfWordCount(sc: SparkContext): RDD[((String, String), Int)] = {
    val files =
      sc.binaryFiles(config.getPropValues("s3sourcePath"), Runtime.getRuntime.availableProcessors).persist(StorageLevel.MEMORY_ONLY_2)
        .flatMap {
          x =>
            val fileName = x._1.split("/").last.toLowerCase()
            parser(fileName, x._2)
              .split(" ")
              .map(_.replace("\n", " "))
              .flatMap(_.split(" ").map(_.replace("\n", "")))
              .map(x => (x, fileName))

        }
    val pdfRDD = files.map(x => ((x._1, x._2), 1))
    pdfRDD
      .reduceByKey((accumulator, combiner) => accumulator + combiner)
      .sortBy(_._2, false)
  }

  def pdfDFText(sql: SQLContext, sc: SparkContext, pdfPath: String, outPath: String): Unit = {
    val logString: StringBuffer = new StringBuffer()
    val sparkUtils: SparkUtils = new SparkUtils(sc, logString)
    import sql.implicits._
    val pdfRDD = sc.binaryFiles(pdfPath, Runtime.getRuntime.availableProcessors).persist(StorageLevel.MEMORY_ONLY_2)
      .map { x =>
        val fileName = x._1.split("/").last.toLowerCase()
        (
          fileName,
          parser(fileName, x._2).replaceAll("(\r\n|\n\r|\r|\n)", " ").toLowerCase)
      }
    pdfRDD
      .toDF("filename", "pdftext")
      .createOrReplaceTempView("pdf")
    sparkUtils.gzipWriter(outPath,
      sql.sql(
        """
          select filename,
          pdftext
          from pdf
          where pdftext is not null
       """.stripMargin)
    )


  }


}
