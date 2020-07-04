package com.sg.transformers.ocr

import java.io._

import com.sg.transformers.utility.SparkUtils
import com.sg.utils.ConfigProps
import org.apache.pdfbox.io.RandomAccessBuffer
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler


class OCRParserPDF extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps()

  def parserStrip(input: (String, PortableDataStream)): String = {
    val OCRparser: org.apache.tika.parser.pdf.PDFParser = new org.apache.tika.parser.pdf.PDFParser()
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

  def parserWithFormat(input: (String, PortableDataStream)): String = {
    val stream: InputStream = input._2.open()
    val OCRparser: org.apache.pdfbox.pdfparser.PDFParser = new org.apache.pdfbox.pdfparser.PDFParser(new RandomAccessBuffer(stream))
    OCRparser.parse()
    val stringWrite: PDDocument = new PDDocument(OCRparser.getDocument())
    //val stripper: PDFTextStripper = new PDFLayoutTextStripper()
    val stripper: PDFTextStripper = new PDFTextStripper()
    stripper.setSortByPosition(true)
    val pdf = new StringBuffer()
    val t = stripper.getText(stringWrite).split("\n")
    t.foreach(x => {
      pdf.append(x.trim()).append("\n")
    })
    stringWrite.close()
    stream.close()
    pdf.toString()
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
          //parserStrip(fileName, x._2).replaceAll("(\r\n|\n\r|\r|\n)", " ").toLowerCase)
          parserWithFormat(fileName, x._2).toLowerCase)
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
