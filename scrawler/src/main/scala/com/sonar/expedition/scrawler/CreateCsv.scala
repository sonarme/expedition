package com.sonar.expedition.scrawler

import jobs.NewAggregateMetricsJob
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter

object CreateCsv extends App {
    val input = args(0)
    val output = args(1)
    val keys =
        (Set.empty[String] ++ (for (line <- scala.io.Source.fromFile(input).getLines();
                                    key <- NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]]).keySet()) yield key))
                .toSeq.sortBy(_.length)
    val categories =
        (Set.empty[String] ++ (for (line <- scala.io.Source.fromFile(input).getLines();
                                    key <- categorySplit(NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]]))) yield key))
                .toSeq.sortBy(_.length)

    val columnNames = "venueSector3char" :: (categories ++ keys).toList
    val index = columnNames.zipWithIndex.toMap[String, Int]

    val writer = new CSVWriter(new FileWriter(output))
    writer.writeNext(columnNames.toArray)
    try {
        for (line <- scala.io.Source.fromFile(input).getLines()) {
            val json = NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]])
            val vector = new Array[String](index.size)
            json.foreach {
                case (key, value) =>
                    vector(index(key)) = value.toString
            }
            vector(index("venueSector3char")) = json.getOrElse("venueSector", "").toString.take(3)
            val categoriesInJson = categorySplit(json).toSet[String]
            categories foreach {
                category =>
                    if (categoriesInJson(category))
                        vector(index(category)) = categoriesInJson(category).toString
            }
            val csvline = vector.toArray[String]
            writer.writeNext(csvline)
        }
    } finally {
        writer.close()
    }

    def categorySplit(json: java.util.Map[String, Any]) = {
        val cat = json.getOrElse("foursquareCategory", "").toString
        if (cat.isEmpty) Seq.empty else cat.split("\\s*,\\s*").toSeq
    }
}
