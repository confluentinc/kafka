/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util.concurrent.CompletableFuture

import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.raft.KafkaRaftManager
import kafka.server.Server.{BrokerRole, ControllerRole}
import kafka.utils.{Logging, Mx4jLoader}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.raft.RaftConfig

import scala.jdk.CollectionConverters._

class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String],
  kafkaMetricsReporters: Seq[KafkaMetricsReporter]
) extends Server with Logging {

  private val roles = config.processRoles

  if (roles.distinct.length != roles.size) {
    throw new ConfigException(s"Duplicate role names found in roles config $roles")
  }

  if (config.quorumVoters == null || config.quorumVoters.isEmpty) {
    throw new ConfigException(s"You must specify a value for ${RaftConfig.QUORUM_VOTERS_CONFIG}")
  }

  private val (metaProps, offlineDirs) = loadMetaProperties()
  private val metrics = configureMetrics(metaProps)

  AppInfoParser.registerAppInfo(KafkaBroker.metricsPrefix,
    config.controllerId.toString, metrics, time.milliseconds())
  KafkaBroker.notifyClusterListeners(metaProps.clusterId.toString,
    kafkaMetricsReporters ++ metrics.reporters.asScala)

  private val metadataPartition = new TopicPartition(Server.metadataTopicName, 0)
  private val controllerQuorumVotersFuture = CompletableFuture.completedFuture(config.quorumVoters)
  private val raftManager = new KafkaRaftManager(metaProps, metadataPartition, config, time, metrics,
    controllerQuorumVotersFuture)

  val broker: Option[Kip500Broker] = if (roles.contains(BrokerRole)) {
    Some(new Kip500Broker(
      config,
      metaProps,
      raftManager.metaLogManager,
      time,
      metrics,
      threadNamePrefix,
      offlineDirs,
      controllerQuorumVotersFuture,
      Server.SUPPORTED_FEATURES
    ))
  } else {
    None
  }

  val controller: Option[Kip500Controller] = if (roles.contains(ControllerRole)) {
    Some(new Kip500Controller(
      metaProps,
      config,
      raftManager.metaLogManager,
      raftManager,
      time,
      metrics,
      threadNamePrefix,
      CompletableFuture.completedFuture(config.quorumVoters)
    ))
  } else {
    None
  }

  private def configureMetrics(metaProps: MetaProperties): Metrics = {
    KafkaYammerMetrics.INSTANCE.configure(config.originals)
    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new java.util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = KafkaBroker.metricConfig(config)
    val metricsContext = KafkaBroker.createKafkaMetricsContext(metaProps, config)

    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }

  private def loadMetaProperties(): (MetaProperties, Seq[String]) = {
    val logDirs = config.logDirs ++ Seq(config.metadataLogDir)
    val (rawMetaProperties, offlineDirs) = BrokerMetadataCheckpoint.
      getBrokerMetadataAndOfflineDirs(logDirs, false)

    if (offlineDirs.contains(config.metadataLogDir)) {
      throw new RuntimeException("Cannot start server since `meta.properties` could not be " +
        s"loaded from ${config.metadataLogDir}")
    }

    (MetaProperties.parse(rawMetaProperties, roles.toSet), offlineDirs.toSeq)
  }

  def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    if (broker.isEmpty) {
      info(KafkaBroker.STARTED_MESSAGE)
    }
  }

  def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    raftManager.shutdown()
    controller.foreach(_.shutdown())
  }

  def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

}

object KafkaRaftServer {
  val MetadataTopic = "@metadata"
  val MetadataPartition = new TopicPartition(MetadataTopic, 0)
}
