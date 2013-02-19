package com.sonar.expedition.common.adx.storm

import org.scala_tools.time.Imports._

import backtype.storm.utils.Utils

import grizzled.slf4j.Logging
import com.sonar.dossier.util.StaticApplicationContext
import com.sonar.dossier.dto.environment.DossierProfiles
import backtype.storm._
import collection.JavaConversions._
import backtype.storm.generated.StormTopology
import scala.Predef._
import scala.Boolean
import topology.TopologyBuilder
import storm.kafka.{KafkaSpout, SpoutConfig}
import storm.kafka.KafkaConfig.ZkHosts

object SubmitTopology extends Logging {

    case class TopologyConfig(workers: Int, maxSpoutPending: Int = 500, messageTimeoutSecs: Int = 30, tickTupleFreq: Option[Duration] = None)

    val topologies = Map(
        "bidRequests" ->
                (new BidRequestTopology,
                        Map(DossierProfiles.STAG_ENV -> TopologyConfig(1, maxSpoutPending = 10),
                            DossierProfiles.PROD_ENV -> TopologyConfig(7, maxSpoutPending = 30, messageTimeoutSecs = 120)
                        )
                        )
    )

    class BidRequestTopology extends Topology {

        val pendingSonarUsersSpoutParallelism = 1
        //we may only want one of these, since we are doing stateful pagination!!!
        val timeSinceLastPong = 3.hours.toPeriod


        def build(parallelismFactor: Int, local: Boolean) = {
            val t = new TopologyBuilder
            val spoutConfig = new SpoutConfig(
                new ZkHosts("107.22.18.19:2181", "/brokers"),
                //new StaticHosts(Seq(new HostPort("kafkahost1")), 8), // list of Kafka brokers and  number of partitions per host
                "bidRequests_prod", // topic to read from
                "/kafkastorm", // the root path in Zookeeper for the spout to store the consumer offsets
                "discovery")
            // an id for this consumer for storing the consumer offsets in Zookeeper
            val kafkaSpout = new KafkaSpout(spoutConfig)
            t.setSpout(classOf[KafkaSpout].getSimpleName, kafkaSpout, 1) // the emits data from a direct pong (via SQS queue)

            t.setBolt(classOf[BidRequestWriterBolt].getSimpleName, new BidRequestWriterBolt, 100)
                    .shuffleGrouping(classOf[KafkaSpout].getSimpleName)
            t.createTopology()
        }
    }

    def construct(local: Boolean, toconstruct: Iterable[(Topology, TopologyConfig)]) = {
        toconstruct.map {
            case (t, config) => t ->(t.build(config.workers, local), config)
        }

    }

    def main(args: Array[String]): Unit = {
        // TODO: use commons-cli or something
        val environment =
            if (args != null && args.length > 0) {
                args(0).toLowerCase
            } else {
                DossierProfiles.DEV_ENV
            }
        //todo: is this necessary"
        System.setProperty(DossierProfiles.AWS_ENV_PARAM, environment)
        info("Running in environment " + environment)


        val baseConfig = Map(StaticApplicationContext.STORM_ENV -> environment,
            StaticApplicationContext.AWS_ACCESS_KEY_ID -> args(1),
            StaticApplicationContext.AWS_SECRET_KEY -> args(2))

        val selectedTopologies = if (args.length > 3) {
            val split = args(3).split(",").toSet
            topologies.filter {
                case (key, top) => split(key)
            }.values
        } else topologies.values

        //todo: move these settings (such as number of workers or max parallelism to zookeeper call or storm conf
        val local = !(environment == DossierProfiles.STAG_ENV || environment == DossierProfiles.PROD_ENV)

        val constructedTopologies = construct(local, selectedTopologies.map {
            case (topology, map) =>
                val environmentToUse = if (environment == DossierProfiles.DEV_ENV) DossierProfiles.STAG_ENV else environment
                val config = map.getOrElse(environmentToUse, TopologyConfig(1))
                topology -> config
        })

        def toConfig(config: TopologyConfig) = {
            val topologyConfig = new Config

            /*     can't get this to work :(
            // beware of scala inner classes and their Java representation
            topologyConfig.put(Config.TOPOLOGY_KRYO_REGISTER, Seq[java.util.Map[String, String]](
                Map(classOf[DateTime].getCanonicalName -> classOf[JodaDateTimeSerializer].getCanonicalName),
                Map("scala.collection.JavaConversions$SetWrapper" -> classOf[HashSetSerializer].getCanonicalName),
                Map("scala.collection.JavaConversions$SeqWrapper" -> classOf[ArrayListSerializer].getCanonicalName)
            ): java.util.List[_])
            topologyConfig.setSkipMissingKryoRegistrations(false)*/
            topologyConfig.setMaxSpoutPending(config.maxSpoutPending)
            topologyConfig.setNumWorkers(config.workers)
            topologyConfig.setMessageTimeoutSecs(config.messageTimeoutSecs)
            config.tickTupleFreq foreach {
                freq =>
                    topologyConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, freq.getStandardSeconds.toInt: java.lang.Integer)
            }
            topologyConfig
        }

        if (!local) {
            val clusterName: String = args(0)
            val remoteConfig = Utils.readStormConfig().map {
                case (key, value) => key.toString -> value.asInstanceOf[AnyRef]
            } ++ baseConfig ++ Map(
                Config.SUPERVISOR_WORKER_TIMEOUT_SECS -> (15.minutes.seconds.toInt: Integer)) //todo: make sure we update this in storm.conf
            info("Using config: " + remoteConfig)

            constructedTopologies.foreach {
                case (topology, (t, config)) => {
                    val topologyConfig = toConfig(config)
                    StormSubmitter.submitTopology(clusterName + "-" + topology.name, remoteConfig ++ topologyConfig, t)
                }
            }

        } else {
            val cluster = new LocalCluster
            try {

                constructedTopologies.foreach {
                    case (topology, (t, config)) => {
                        val topologyConfig = toConfig(config)
                        cluster.submitTopology("local-" + topology.name, baseConfig ++ topologyConfig, t)
                        topology.postProcess()
                    }
                }

                Thread sleep 999000
            } finally {
                cluster.shutdown
            }

        }
    }

}

trait Topology {
    val name = getClass.getSimpleName

    def build(parallelismFactor: Int, local: Boolean): StormTopology

    def postProcess() {}
}
