package com.example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class WineItem(id: Option[Integer],
                    country: Option[String],
                    points: Option[Integer],
                    price: Option[Number],
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

    for (line <- items) {

      val json: json4s.JValue = parse(line)
      val id: Integer = (json \ "id").extractOrElse(0)
      val country: String = (json \ "country").extractOrElse("N/A")
      val points: Integer = (json \ "points").extractOrElse(0)
      val price: Integer = (json \ "price").extractOrElse(0)
      val title: String = (json \ "title").extractOrElse("N/A")
      val variety: String = (json \ "variety").extractOrElse("N/A")
      val winery: String = (json \ "winery").extractOrElse("N/A")

      println("id: " + id +
        " | country: " + country +
        " | points: " + points +
        " | title: " + title +
        " | variety: " + variety +
        " | winery: " + winery)
    }
    sc.stop()
}

