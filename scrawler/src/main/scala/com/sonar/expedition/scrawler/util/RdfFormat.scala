package com.sonar.expedition.scrawler.util

object RdfFormat extends Enumeration {
    val Turtle = Value("TURTLE")
    val RdfXmlAbbrev = Value("RDF/XML-ABBREV")
    val Ntriple = Value("N-TRIPLE")
}