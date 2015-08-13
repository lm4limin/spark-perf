package mllib.perf.util

import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import scala.util.{Try,Success, Failure}

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
    val parsedData = data.map{s => 
      val ast = parse(s).asInstanceOf[JObject]
      val str = (Try((ast \ "body").extract[String]).recover {case e => ""}).transform({i=>Success(i)},{e=>Success(e)}).get
//      val tweet= (ast \ "body").extract[String]
      //println(str)
      //pretty(ast)
      //if(str!="") 
        //(str.toString).filter(_.length>0).filter( _ > ' ')
//        res=str.toString.filter( _ > ' ')
	//val res=str.toString.toLowerCase().replaceAll("[^ A-Za-z0-9]+", "")
	val res=str.toString.toLowerCase().replaceAll("[^ A-Za-z:/]+", "")
//	println(res)
        res
    }.filter(_.length>0)//.filter(_ >' ')
//     }
    parsedData.saveAsTextFile("/small-1k-output.txt")
    
    
  }
}
