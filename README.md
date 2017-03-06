#  xRay 
### Extract metadata from your S3 buckets and analyze in hadoop or search in elastic search


xRay enables simple extraction of your S3 metadata into two data formats
1. Parquet files that you can import into a spark cluster for analytics
2. An elastic search file to import into elastic search 

Quick Start

1. Generate a binary for your system 
```
$ git clone https://github.com/vardhanv/xray.git
$ cd xray
$ sbt universal:packageBin
$ cd target/universal
$ unzip xray-<version>.zip
$ cd xray-<version>/bin
$ ./xray --help
```

2. If you generate an elastic search output file (assume xray.out)
  a. Create an elastic search cluster on AWS
  b. Upload the data into elastic search
```
$ curl --tr-encoding -XPOST 'http://<your_elastic_search_url>/_bulk' --data-binary @xray.out
```
  c. Now you can analyze it in kibana that AWS provides
3. If you generate a parquet file you can analyze it in a spark cluster
  a. Go to http://www.databricks.com
  b. Click on "Manage Account" 
  c. Select community edition
  d. Create a cluster - wait for the cluster to come online
  e. Create a table using the parquet file - (assume "giab")
  f. Create a notebook - Workspare/users/.../Create/Notebook/Language Scala
```
> import sqlContext.implicits._
> import org.apache.spark.sql.functions._
> val sqlContext = new org.apache.spark.sql.SQLContext(sc)
> val df = sqlContext.table("giab")
> df.count
> df.show()
> // Deduplicated Storage Used in TB
> val TB : Long = 1000000000000L
> val totalTB  = df.select(col("Content_Length")).rdd.map(_(0).asInstanceOf[Long].toDouble/TB).reduce(_+_)
> val totalTB_unique = df.dropDuplicates("ETag").select(col("Content_Length")).rdd.map(_(0).asInstanceOf[Long].toDouble/TB).reduce(_+_)
> val totalSavings = totalTB - totalTB_unique
```

USAGE
```
$ ./xray --help
xRay 1.0
Usage: xRay [options]

  -b, --bucket <value>     target s3 bucket
  -p, --parquet            generate parquet file output
  -l, --elastic            generate elastic search output
  -x, --number-obj:maxObj=objAtATime
                           optional, <x=y>, index "x" objects "y" at a time. defaults: x [1 billion], y:[1000]
  -e, --ep-url <value>     optional, endpoint. default = https://s3.amazonaws.com
  -f, --profile <value>    optional, aws profile. default = default. create using "aws --configure"
  -r, --region <value>     optional, s3 region.
  -o, --output <value>     optional, output file. default = xray.out
  --help                   prints help text
```
