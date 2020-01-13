package com.example

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class WineItem(id: Option[Long],
                    country: Option[String],
                    points: Option[Long],
                    price: Option[Double],
                    title: Option[String],
                    variety: Option[String],
                    winery: Option[String])

object JsonReader extends App {

    val conf = new SparkConf().setMaster("local[*]").setAppName("JsonReader")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    implicit val json4sFormats = DefaultFormats

    val filepath = args(0)
    val items = sc.textFile(filepath)

  def show_value(x: Any) = x match {
    case Some(s) => s
    case None => "N/A"
  }

  for (line <- items) {
      println(
          "id: " + show_value(parse(line).extract[WineItem].id) +
          " | country: " + show_value(parse(line).extract[WineItem].country) +
          " | points: " + show_value(parse(line).extract[WineItem].points) +
          " | price: " + show_value(parse(line).extract[WineItem].price) +
          " | title: " + show_value(parse(line).extract[WineItem].title) +
          " | variety: " + show_value(parse(line).extract[WineItem].variety) +
          " | winery: " + show_value(parse(line).extract[WineItem].winery)
      )
    }

    sc.stop()
}

