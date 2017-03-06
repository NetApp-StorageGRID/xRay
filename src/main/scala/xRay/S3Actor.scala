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

import scala.concurrent.ExecutionContext
import com.amazonaws.services.s3.{ AmazonS3Client, AmazonS3ClientBuilder }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.{ ListObjectsRequest, ObjectListing }

import scala.concurrent.{ Future, blocking, Await, duration }
import java.util.concurrent.Executors
import org.apache.log4j._


object S3Actor {

  val executors = Executors.newFixedThreadPool(100)
  val blockingEC = ExecutionContext.fromExecutor(executors)
  val log = Logger.getLogger(this.getClass)
  
  // required to access AWS credentials from a docker container
  log.debug("loading environment variables.. ")

  private val s3Client = AmazonS3ClientBuilder
    .standard()
    .withEndpointConfiguration(new EndpointConfiguration(MyConfig.cmdLine.endpoint, MyConfig.cmdLine.region))
    .withCredentials(new ProfileCredentialsProvider(MyConfig.cmdLine.awsProfile))
    .build()

  def listObjects(bucketName: String, key: String, marker: String, delimiter: String, maxKeys: Int) =
    Future {
      blocking { s3Client.listObjects(new ListObjectsRequest(bucketName, key, marker, delimiter, maxKeys)) }
    }(blockingEC)

  def getObjectMetadata(bucket: String, key: String) =
    Future {
      blocking { s3Client.getObjectMetadata(bucket, key) }
    }(blockingEC)

  def close() = executors.shutdown()
}