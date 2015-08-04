package mllib.perf.util
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

object DataTwitterETL{
  implicit val formats = DefaultFormats

  def run(): Unit = {
    val json = """{"foo":1, "bar":{"foo":2}}"""
    val ast = parse(json).asInstanceOf[JObject]

    val updated = ast merge (("foo", 3) ~ ("bar", ("fnord", 5)))

    println(pretty(updated))
  }
}