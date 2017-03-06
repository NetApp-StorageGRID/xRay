/*
 * Copyright <2017> <Vishnu Vardhan Chandra Kumaran>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of 
 * this software and associated documentation files (the "Software"), to deal in the 
 * Software without restriction, including without limitation the rights to use, copy, 
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, 
 * and to permit persons to whom the Software is furnished to do so, subject to the 
 * following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies
 *  or substantial portions of the Software.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
 *  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
 *  PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *   LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT 
 *   OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 *   DEALINGS IN THE SOFTWARE.
*/



package xRay


import org.apache.log4j._

import scala.collection.JavaConversions

import java.io.File
import org.apache.hadoop.fs.Path

import java.util.Date


import org.apache.parquet.avro.{ AvroWriteSupport, AvroSchemaConverter, AvroParquetWriter }
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata._

import org.apache.avro.generic.{ GenericData, GenericRecordBuilder }
import org.apache.avro.Schema

//Json Formaters

import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json._
import DefaultJsonProtocol._
import java.nio.file.Files
import java.nio.file.FileSystems
import java.nio.file.StandardOpenOption



class MD(m: List[Map[String, Object]]) {
  val md = m

  def toJson: List[JsValue] = md.map (elemJson(_)).toList

  def elemJson(md: Map[String, Object]): JsValue = md.map {
    case (k: String, d: Date) => (k -> d.getTime.toJson)
    case (k: String, s: String) => (k -> s.toJson)
    case (k: String, l: java.lang.Long) => (k -> l.toLong.toJson)
    case (k: String, m: UserMD) => {
      val value = m.md.map { case (k: String, v: String) => (k -> v.toJson) }.toJson
      (k -> value)
    }
    case (k: String, usrMd: Any) => (k -> "UNKNOWN".toJson)
  }.toJson

}


// see: http://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
// and: https://avro.apache.org/docs/1.8.1/spec.html

trait MyFormatWriter { 
  def write(metadata: MD)
  def close()  
}

class MyParquet extends MyFormatWriter {

  val log = Logger.getLogger(classOf[MyParquet])

  val tmp = new File(MyConfig.cmdLine.outputFile)
  tmp.delete()
  val outputPath = new org.apache.hadoop.fs.Path(tmp.getPath)

  // schema is an array of map's
  // Load your avro schema
  val avroSchema = new Schema.Parser().parse("""
						|{ "namespace" : "xRay.avro", 
						| "type" : "record", 
						|  "name" : "Metadata", 
						| "fields" : [ 
						|      { "name" : "Last_Modified",     "type":"long", "logicalType":"timestamp-millis" },
						|      { "name" : "Accept_Ranges",     "type":"string" },
						|      { "name" : "Content_Length",    "type":"long"    }, 
						|      { "name" : "key",               "type":"string" }, 
						|      { "name" : "Content_Type",      "type":"string" },  
						|      { "name" : "x_amz_version_id",  "type":["null","string"] },
						|      { "name" : "bucket",            "type":"string" },
						|      { "name" : "ETag",              "type":"string" },
						|      { "name" : "User_MD",           "type":{ "type":"map", "values":"string" }}
						|  ]
						|}
						|""".stripMargin)

  val parquetWriter_ = AvroParquetWriter
    .builder[GenericData.Record](outputPath)
    .withSchema(avroSchema)
    .withCompressionCodec(CompressionCodecName.SNAPPY)
    .build()

  def write(metadata: MD) = {
    try {
      log.debug("write called")

      metadata.md.map(mdMap => {
        val usrMD = new GenericData.Record(avroSchema)

        mdMap.map { x =>

          val valueToWrite = x match {
            case (_,d: Date)   => d.getTime
            case (_,u: UserMD) => JavaConversions.mapAsJavaMap(u.md)
            case (_,l: java.lang.Long) => l.toLong
            case (_,s: String) => s
            case (_,a: Any)    => { log.debug("unknown class = " + a.getClass()); a }
          }
           
          usrMD.put(x._1.replace("-", "_"), valueToWrite)
        }

        parquetWriter_.write(usrMD)
      })
    } catch { case e: Any => log.error("Exception:  Check your schema format, " + e) }
  }

  // close this writer
  def close() = parquetWriter_.close
}

// Json Support
// see: http://doc.akka.io/docs/akka-http/10.0.4/scala/http/common/json-support.html

object MDtoJson {
  def toJson(md: Map[String, Object]): JsValue = md.keySet.map { x =>
      md(x) match {
        case d: Date => (x -> d.getTime.toJson)
        case s: String => (x -> s.toJson)
        case l: java.lang.Long => (x -> l.toLong.toJson)
        case usrMd: Any => (x -> "UNKNOWN".toJson)
      }
    }.toMap.toJson
}



class MyJson extends MyFormatWriter {

  val log = Logger.getLogger(classOf[MyJson])
  
  val outputPath = FileSystems.getDefault().getPath(MyConfig.cmdLine.outputFile)  
  // delete any existing file  
  Files.write(outputPath, "".getBytes, StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)
  
  
  def write(metadata: MD) =  try {
      metadata.toJson.foreach { x =>
        val strToWrite = (x.toString + "\n")
        
        Files.write(outputPath, strToWrite.getBytes, 
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND) 
      }
    } catch { case e: Any => log.error("Exception: Unable to create Json, " + e) }


  // close this writer
  def close() = {}   
}



class MyElastic extends MyFormatWriter {

  val log = Logger.getLogger(classOf[MyJson])
  
  val outputPath = FileSystems.getDefault().getPath(MyConfig.cmdLine.outputFile)  
  // delete any existing file  
  Files.write(outputPath, "".getBytes, StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING)
  
  def write(metadata: MD) = try {
    metadata.md.map { m =>
      val esHdr = """{ "index" : { "_index": "xray", "_type": "S3Object", "_id": """" +        
        m("bucket")+ "/" + m("key") + "\" } } "

        
      val strToWrite = esHdr + "\n" + metadata.elemJson(m).toString() + "\n"
      Files.write(outputPath, strToWrite.getBytes,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND)
    }
  } catch { case e: Any => log.error("Exception: Unabe to create ElasticSearch bulk upload file" + e) }

  // close this writer
  def close() = {}   
}