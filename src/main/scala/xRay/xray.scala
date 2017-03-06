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

import com.amazonaws.services.s3.model.ObjectListing

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._

import scala.concurrent.{ Future, blocking, Await, duration }
import scala.concurrent.duration._
import scala.concurrent.forkjoin._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{ Try, Failure, Success }

//Actor Streams imports
import akka.stream.{ ActorMaterializer }
import akka.stream.scaladsl.{ Source, Sink, Flow, RunnableGraph, GraphDSL, Keep }
import akka.stream.Attributes._
import akka.actor.ActorSystem


object MyConfig {  
  var cmdLine     = CmdLineConfig()
  val config      = ConfigFactory.load().getConfig("xRay")  
}

// use this to prevent the warning 
// see: http://stackoverflow.com/questions/25222989/erasure-elimination-in-scala-non-variable-type-argument-is-unchecked-since-it
final case class UserMD(md: Map[String, String])


object xRay {
  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(this.getClass)
        
    MyConfig.cmdLine = CmdLineParser.parseCmdLine(args).getOrElse(CmdLineConfig())
        
    implicit val system = ActorSystem("xRay-ActorSystem")
    implicit val materializer = ActorMaterializer()
    val writer : MyFormatWriter = if (MyConfig.cmdLine.parquet) new MyParquet() else new MyElastic()
    
    
    log.info("Hello, xRay is ready!")

    // Acts as a source of ObjectListing that it gets from AWS
    // never ends unless the listObject call fails

    val objectListingSrc = Source.unfoldAsync(Some(""): Option[String]) { next =>
      if (next.isEmpty == true) Future { Some(None -> None) }
      else
        S3Actor
          .listObjects(MyConfig.cmdLine.bucket, null, next.get, null, MyConfig.cmdLine.maxObjPerList.toInt)
          .map(x => {
            val gen = if (x.isTruncated == false) None else Some(x.getNextMarker())
            Some(gen -> Some(x))
          })
    }

    // takes an objectList from AWS S3 listObject, and converts it into a map of the metadata's
    // in the list step it converts an a Future into a flow
    val getObjMetaData = Flow.fromFunction[Option[ObjectListing], Future[MD]](objList =>
      Future.sequence(
          objList.get.getObjectSummaries.map( obj =>
            S3Actor
              .getObjectMetadata(MyConfig.cmdLine.bucket, obj.getKey)
              .map(md =>
                
                    (md.getRawMetadata.toMap)
                  + ("User_MD" -> UserMD(md.getUserMetadata.toMap))
                  + ("bucket" -> MyConfig.cmdLine.bucket)
                  + ("key" -> obj.getKey)
                  )
            ).toList            
        ).map( y => new MD(y) )  // convert a List[Future] to Future [List] and then map to MD              
      ).mapAsync(1)(x => x)  // create a flow

    // md sink
    val mdSink = Sink.fold(0) { (count, x: MD) =>
      {
        val f : Float = count + x.md.length
        writer.write(x)
        log.warn("Completed : "  + (count + x.md.length) + " objects, " + ((f/MyConfig.cmdLine.maxObj)*100).toInt + "% of "+ MyConfig.cmdLine.maxObj + " objects")
        count + x.md.length
      }
    }

    val startTime = System.nanoTime()    
    val iterations = MyConfig.cmdLine.maxObj / MyConfig.cmdLine.maxObjPerList
    
    // run the flow
    val myStream = objectListingSrc
      .takeWhile(_.exists((x) => true))
      .take( if (iterations < 1) 1 else iterations )
      .via(getObjMetaData)
      .via(Flow[MD].log("StrLog"))
      .runWith(mdSink)

    myStream.onComplete {
      case Success(x) => {
        println("Total runtime = " + (System.nanoTime() - startTime) / 1000000 + "ms")
        shutdownAll()
      }
      case Failure(x) => {
        log.error("Failure : " + x.getMessage)
        shutdownAll()
      }
    }

    def shutdownAll() {
      writer.close()
      S3Actor.close()
      system.terminate()
    }
  }
  
}

