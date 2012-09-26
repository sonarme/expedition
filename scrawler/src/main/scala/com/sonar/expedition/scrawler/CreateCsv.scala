package com.sonar.expedition.scrawler

import jobs.NewAggregateMetricsJob
import collection.JavaConversions._
import collection.mutable._
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import collection.mutable

object CreateCsv extends App {
    val input = args(0)
    val output = args(1)
    val arfftypeEls = collection.mutable.Map[String, mutable.HashSet[Any]]()
    val keys =
        (Set.empty[String] ++ (for (line <- scala.io.Source.fromFile(input).getLines();
                                    (key, value) <- NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]])) yield {
            val list = arfftypeEls.getOrElse(key, HashSet.empty[Any])
            if (list.size < 70) list += (value match {
                case s: String => clean(s)
                case x => x
            })
            arfftypeEls(key) = list
            key
        }))
                .toSeq.sortBy(_.length)
    val arfftypes = arfftypeEls.toSeq.sortBy(_._1.length) map {
        case (key, values) =>
            key -> {
                values.headOption match {
                    case Some(_: Number) => "NUMERIC"
                    case Some(_: String) => if (values.size >= 70) "string"
                    else values.map("'" + _ + "'").mkString("{", ",", "}")
                    case _ => "UNKNOWN"
                }
            }
    }
    val categories =
        (Set.empty[String] ++ (for (line <- scala.io.Source.fromFile(input).getLines();
                                    key <- categorySplit(NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]]))) yield clean(key)))
                .toSeq.sortBy(_.length)

    val columnNames = "venueSector3char" :: "revenue" :: "dealSuccess" :: "revenueBucket" :: (categories ++ arfftypes.map(_._1)).toList
    val index = columnNames.zipWithIndex.toMap[String, Int]

    val fw = new FileWriter(output)
    val writer = new CSVWriter(fw, ',', CSVWriter.NO_QUOTE_CHARACTER)
    fw.write("@RELATION test\n")
    fw.write("@ATTRIBUTE venueSector3char string\n")
    fw.write("@ATTRIBUTE revenue NUMERIC\n")
    fw.write("@ATTRIBUTE dealSuccess {true,false}\n")
    fw.write("@ATTRIBUTE revenueBucket {0..20}\n")
    categories.foreach {
        case cat =>
            fw.write("@ATTRIBUTE " + cat.replaceAll(" ", "") + " {true,false}\n")
    }
    arfftypes.foreach {
        case (key, value) =>
            fw.write("@ATTRIBUTE " + key + " " + value + "\n")
    }
    fw.write("@DATA\n")

    //writer.writeNext(columnNames.toArray)
    try {
        for (line <- scala.io.Source.fromFile(input).getLines()) {
            val json = NewAggregateMetricsJob.ObjectMapper.readValue(line, classOf[java.util.Map[String, Any]])
            val vector = new Array[String](index.size)
            json.foreach {
                case (key, value) =>
                    vector(index(key)) =
                            value match {
                                case s: String => clean(s)
                                case x => x.toString
                            }
            }
            val cleanedGeo = clean(json.getOrElse("venueSector", "").toString.take(3))
            if (cleanedGeo != "?") {
                vector(index("venueSector3char")) = cleanedGeo
                val purchased = json.get("purchased")
                val minPricepoint = json.get("minPricepoint")
                val revenue = if (purchased != null && minPricepoint != null) {
                    purchased.toString.toInt * minPricepoint.toString.toDouble
                }
                else -1
                vector(index("revenue")) = if (revenue < 0) "?" else revenue.toString
                vector(index("dealSuccess")) = if (revenue < 0) "?" else (revenue > 1800).toString
                vector(index("revenueBucket")) = math.round(math.log(1 + (revenue match {
                    case revenueTier if revenueTier > 1000 => revenueTier
                    case revenueTier if revenueTier < 72 => 0
                    case revenueTier if revenueTier < 360 => 360
                    case _ => 720
                }))).toString
                val categoriesInJson = categorySplit(json).toSet[String]
                categories foreach {
                    category =>
                        if (categoriesInJson(category))
                            vector(index(category)) = categoriesInJson(category).toString
                }
                //val shortened= vector.dropRight(vector.reverse.takeWhile(_ == null).size)
                val sanitized = vector.map {
                    case x if (x == null) => "?"
                    case x => x
                }
                val csvline = sanitized.toArray[String]

                writer.writeNext(csvline)
            }
        }
    } finally {
        writer.close()
    }


    def clean(s: String) = {
        val res = s.replaceAll("[\\s,`'\"/%]", "")
        if (res.isEmpty) "?" else res
    }

    def categorySplit(json: java.util.Map[String, Any]) = {
        val cat = json.getOrElse("foursquareCategory", "").toString
        if (cat.isEmpty) Seq.empty else cat.split("\\s\\s+").toSeq.map(clean)
    }
}
