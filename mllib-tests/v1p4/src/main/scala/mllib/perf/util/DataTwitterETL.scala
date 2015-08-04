package mllib.perf.util

import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

object DataTwitterETL{
  implicit val formats = DefaultFormats

  def run(sc: SparkContext): JValue = {
    val json = """{"foo":1, "bar":{"foo":2}}"""
    val ast = parse(json).asInstanceOf[JObject]

    val updated = ast merge (("foo", 3) ~ ("bar", ("fnord", 5)))

    println(pretty(updated))
    val input="/small-1k.json"
    val data = sc.textFile(input)
    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val parsedData = data.map{s => 
      val ast = parse(s).asInstanceOf[JObject]
      val tweet= (ast \ "body").extract[String]
      println(tweet)
      tweet
    }
    parseData.saveAsTextFile("/small-1k-output.txt")
    
    
  }
}