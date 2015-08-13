package mllib.perf.util

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import scala.util.{Try,Success, Failure}
import scala.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext,SparkConf, Logging}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel

object DataTwitterETL{
  implicit val formats = DefaultFormats

  def run(sc: SparkContext): Unit = {
    val json = """{"foo":1, "bar":{"foo":2}}"""
    val ast = parse(json).asInstanceOf[JObject]

    val updated = ast merge (("foo", 3) ~ ("bar", ("fnord", 5)))

    println(pretty(updated))
    val input="/small-1k.json"
    val data = sc.textFile(input)
    //val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
    val parsedData1 = data.map{s => 
      val ast = parse(s).asInstanceOf[JObject]
      val str = (Try((ast \ "body").extract[String]).recover {case e => ""}).transform({i=>Success(i)},{e=>Success(e)}).get
//      val tweet= (ast \ "body").extract[String]
      //println(str)
      //pretty(ast)
      //if(str!="") 
        //(str.toString).filter(_.length>0).filter( _ > ' ')
//        res=str.toString.filter( _ > ' ')
	//val res=str.toString.toLowerCase().replaceAll("[^ A-Za-z0-9]+", "")
	val res=str.toString.toLowerCase().replaceAll("[^ A-Za-z:/]+", "").trim
//	println(res)
        res
    }.filter(_.length>0)//.filter(_ >' ')
//     }
    
    
      // Load documents (one per line).
  val documents:RDD[Seq[String]] = 
    parsedData1.map{ line => line.split("").toSeq    }
  
  documents.persist(StorageLevel.MEMORY_AND_DISK)
  
  //val documents: RDD[Seq[String]] = parsedData.map{ case (cate,doc) => doc} 
  val hashingTF = new HashingTF()
  val tf: RDD[Vector] = hashingTF.transform(documents)
  tf.cache()
  val idf = new IDF(minDocFreq = 2).fit(tf)
  val tfidf: RDD[Vector] = idf.transform(tf)
  
  val results: RDD[LabeledPoint]=tfidf.map{point =>   
    val rnd = new Random(System.currentTimeMillis())
    val yD=rnd.nextGaussian() 
    val y = if (yD < 0) 0.0 else 1.0
    new LabeledPoint(y,point)
    
  }

    results.saveAsTextFile("/small-1k-output.txt")
    
    
  }
}
