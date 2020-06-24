package com.sg.transformers.ocr

import java.io._

import com.sg.transformers.utility.SparkUtils
import com.sg.utils.ConfigProps
//import net.sourceforge.tess4j.Tesseract
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.tika.Tika
import org.apache.tika.exception.TikaException
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.image.ImageParser
import org.apache.tika.sax.BodyContentHandler


class OCRParserImage extends java.io.Serializable {

  val config: ConfigProps = new ConfigProps()

  def parser(input: (String, PortableDataStream)): String = {

    val imageParser: ImageParser = new ImageParser
    val tika: Tika = new Tika()
    val stream: InputStream = input._2.open()
    val stringWrite: StringWriter = new StringWriter()
    val handler: BodyContentHandler = new BodyContentHandler(stringWrite)
    val metadata: Metadata = new Metadata()
    val context: ParseContext = new ParseContext()
    val image = new StringBuffer

    try {
      imageParser.parse(stream, handler, metadata, context)
      image.append(input._1.replace("file:", "") + ": " + "Type formatted detected -> " + tika.detect(input._1.replace("file:", "")))
        .append(System.lineSeparator)
        .append(handler.toString)
        .append(metadata.names().foreach(x => image.append(metadata.get(x)).append(System.lineSeparator())))
        .toString()
    }
    catch {
      case exception: TikaException => println(exception.getMessage)
    }
    finally {
      stream.close()
      stringWrite.close()
    }
    image.toString
  }

  def imageDFText(ss: SQLContext, sc: SparkContext): Unit = {
    import ss.implicits._
    val logString: StringBuffer = new StringBuffer()
    val sparkUtils: SparkUtils = new SparkUtils(sc, logString)
    val imageRDD = sc.binaryFiles(config.getPropValues("s3sourcePathImage"), Runtime.getRuntime.availableProcessors).persist(StorageLevel.MEMORY_ONLY_2)
      .map { x =>
        val fileName = x._1.split("/").last.toLowerCase()
        (
          fileName,
          parser(fileName, x._2).replaceAll("(\r\n|\n\r|\r|\n)", " ").toLowerCase
        )
      }
      .toDF("filename", "text")
    imageRDD.show(10, false)
  }


}
