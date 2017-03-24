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

// scopt for commandline
import scopt._
import org.apache.log4j._

case class CmdLineConfig(
  endpoint: String = "https://s3.amazonaws.com",
  region: String = "us-east-1",
  awsProfile: String = "default",
  bucket: String = "",
  maxObj: Long = 10000000000L,
  maxObjPerList: Long = 1000L,
  parquet: Boolean = false,
  elastic: Boolean = false,
  outputFile: String = "./xRay.out")

object CmdLineParser {

  def parseCmdLine(a: Array[String]): Option[CmdLineConfig] = {

    val log = Logger.getLogger(this.getClass)

    val cmdLineParser = new scopt.OptionParser[CmdLineConfig]("xRay") {
      head("xRay", "1.0")

      opt[String]('b', "bucket")
        .required()
        .action((x, c) => c.copy(bucket = x))
        .text("target s3 bucket")

      opt[Unit]('p', "parquet")
        .action((_, c) => c.copy(parquet = true))
        .text("generate parquet file output")

      opt[Unit]('l', "elastic")
        .action((x, c) => c.copy(elastic = true))
        .text("generate elastic search output")

      opt[(Int, Int)]('x', "number-obj")
        .action({ case ((k, v), c) => c.copy(maxObj = k, maxObjPerList = v) })
        .validate({
          case (k, v) =>
            if (k <= -1L) failure("max-obj >= 0 <= 1 billion")
            else if (v <= -1L || v >= 1000L) failure("obj-at-a-time <= 1000 >= 1")
            else success
        })
        .optional
        .keyValueName("maxObj", "objAtATime")
        .text("optional, <x=y>, index \"x\" objects \"y\" at a time. defaults: x [1 billion], y:[1000]")

      opt[String]('e', "ep-url")
        .action((x, c) => c.copy(endpoint = x))
        .optional()
        .text("optional, endpoint. default = https://s3.amazonaws.com")

      opt[String]('f', "profile")
        .action((x, c) => c.copy(awsProfile = x))
        .optional()
        .text("optional, aws profile. default = default. create using \"aws --configure\"")

      opt[String]('r', "region")
        .action((x, c) => c.copy(region = x))
        .optional()
        .text("optional, s3 region.")

      opt[String]('o', "output")
        .action((x, c) => c.copy(outputFile = x))
        .optional()
        .text("optional, output file. default = xray.out")

      help("help").text("prints help text")

      checkConfig(c =>
        if ((c.parquet && c.elastic) || !(c.parquet || c.elastic)) failure("select one output format parquet(\"-p\") or elastic(\"-e\")")
        else success)

    }

    cmdLineParser.parse(a, CmdLineConfig()) match {
      case Some(config) =>
        log.debug("Parsed cmdline: " + config)
        Some(config)
      case None =>
        None
    }

  }

}