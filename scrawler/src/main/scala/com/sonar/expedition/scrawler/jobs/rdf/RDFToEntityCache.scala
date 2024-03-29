package com.sonar.expedition.scrawler.jobs.rdf

import org.apache.hadoop.fs.{Path, FileSystem}
import com.twitter.scalding._
import de.fuberlin.wiwiss.silk.config.LinkingConfig
import de.fuberlin.wiwiss.silk.util.DPair
import de.fuberlin.wiwiss.silk.plugins.jena.JenaPlugins
import de.fuberlin.wiwiss.silk.datasource.{Source, DataSource}
import de.fuberlin.wiwiss.silk.hadoop.impl.HadoopEntityCache
import de.fuberlin.wiwiss.silk.util.sparql.EntityRetriever
import org.apache.hadoop
import grizzled.slf4j.Logging
import de.fuberlin.wiwiss.silk.plugins.Plugins
import de.fuberlin.wiwiss.silk.plugins.jena.RdfDataSource
import scala.Some
import com.twitter.scalding.TextLine
import com.sonar.expedition.scrawler.jobs.DefaultJob


class RDFToEntityCache(args: Args) extends DefaultJob(args) with Logging {
    val silkConfigPath = args("silkConfigPath")
    val entityCachePathArg = args("entityCachePath")
    val linkSpecArg = args.optional("linkSpec")
    val buckets = 100

    def copyConfig() {

        val filePath = new Path(silkConfigPath)
        val entityCachePath = new Path(entityCachePathArg)
        val hadoopConfig = new hadoop.conf.Configuration
        //Create two FileSystem objects, because the config file and the entity cache might be located in different file systems
        val configFS = FileSystem.get(filePath.toUri, hadoopConfig)
        val cacheFS = FileSystem.get(entityCachePath.toUri, hadoopConfig)

        //Copy the config file into the entity cache directory
        val inputStream = configFS.open(filePath)
        val outputStream = cacheFS.create(entityCachePath.suffix("/config.xml"))
        try {
            val buffer = new Array[Byte](4096)
            var c = inputStream.read(buffer)
            while (c != -1) {
                outputStream.write(buffer, 0, c)
                c = inputStream.read(buffer)
            }
        }
        finally {
            outputStream.close()
            inputStream.close()
        }

    }

    copyConfig()

    TextLine(args("rdf")).read
            // create buckets for the n-triples by subject (doesn't work for all sparql queries)
            .flatMap('line -> 'hashBucket) {
        line: String =>
            if (line.isEmpty) None
            else {
                val Array(subject, rest) = line.split(" ", 2)
                Some(subject.hashCode % buckets)
            }
    }
            // bucketize
            .groupBy('hashBucket) {
        _.mapList('line -> '__dummy__) {
            lines: List[String] =>

            // need to load silk stuff here and not at the beginning of the job, because some of it isn't serializable
                Plugins.register()
                JenaPlugins.register()
                DataSource.register(classOf[RdfDataSource])
                EntityRetriever.useParallelRetriever = false
                //Load the configuration
                val silkConfig = {
                    val filePath = new Path(silkConfigPath)
                    val configFS = FileSystem.get(filePath.toUri, new hadoop.conf.Configuration)
                    val stream = configFS.open(filePath)
                    try {
                        LinkingConfig.load(stream)
                    } finally {
                        stream.close()
                        configFS.close()
                    }
                }
                val linkSpec = (linkSpecArg match {
                    case Some(spec) => silkConfig.linkSpec(spec) :: Nil
                    case None => silkConfig.linkSpecs
                }).head
                val sources = linkSpec.datasets.map(_.sourceId).map(silkConfig.source(_))
                val entityDesc = linkSpec.entityDescriptions

                val entityCachePath = new Path(entityCachePathArg)
                val cacheFS = FileSystem.get(entityCachePath.toUri, new hadoop.conf.Configuration)
                val caches = DPair(
                    new HadoopEntityCache(entityDesc.source, linkSpec.rule.index(_), cacheFS, entityCachePath.suffix("/source/" + linkSpec.id + "/"), silkConfig.runtime),
                    new HadoopEntityCache(entityDesc.target, linkSpec.rule.index(_), cacheFS, entityCachePath.suffix("/target/" + linkSpec.id + "/"), silkConfig.runtime)
                )
                for (selectSource <- Seq(true, false)) {

                    val entityCache = caches.select(selectSource)
                    val source = sources.select(selectSource) match {
                        case Source(id, rdfDataSource: RdfDataSource) => Source(id, new RdfDataSource(lines.mkString("\n"), rdfDataSource.format))
                        case _ => throw new RuntimeException("Only supports RdfDataSource")
                    }

                    try {
                        info("Loading entities of dataset")

                        entityCache.clear()
                        entityCache.write(source.retrieve(entityCache.entityDesc))
                        entityCache.close()
                    } catch {
                        case ex: Exception =>
                            warn("Error loading resources", ex)

                    }
                }
                true
        }
    }.write(NullSource)


}
