/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util
import java.util.Collections

import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{ClusterResource, Endpoint}
import org.apache.kafka.metadata.VersionRange
import org.apache.kafka.server.authorizer.AuthorizerServerInfo

import scala.jdk.CollectionConverters._

private[kafka] case class KafkaAuthorizerServerInfo(clusterResource: ClusterResource,
                                                    brokerId: Int,
                                                    endpoints: util.List[Endpoint],
                                                    interBrokerEndpoint: Endpoint) extends AuthorizerServerInfo

trait Server {
  def startup(): Unit
  def shutdown(): Unit
  def awaitShutdown(): Unit

  def initializeMetrics(
    config: KafkaConfig,
    time: Time,
    clusterId: String
  ): Metrics = {
    KafkaYammerMetrics.INSTANCE.configure(config.originals)

    val jmxReporter = new JmxReporter()
    jmxReporter.configure(config.originals)

    val reporters = new util.ArrayList[MetricsReporter]
    reporters.add(jmxReporter)

    val metricConfig = KafkaBroker.metricConfig(config)
    val metricsContext = KafkaBroker.createKafkaMetricsContext(clusterId, config)
    new Metrics(metricConfig, reporters, time, true, metricsContext)
  }
}

object Server {
  val metadataTopicName = "@metadata"

  def apply(
    config: KafkaConfig,
    time: Time = Time.SYSTEM,
    threadNamePrefix: Option[String] = None,
    kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()
  ): Server = {
    val roles = config.processRoles
    if (roles == null || roles.isEmpty) {
      new KafkaServer(config, time, threadNamePrefix, kafkaMetricsReporters)
    } else {
      new KafkaRaftServer(config, time, threadNamePrefix, kafkaMetricsReporters)
    }
  }

  sealed trait ProcessStatus
  case object SHUTDOWN extends ProcessStatus
  case object STARTING extends ProcessStatus
  case object STARTED extends ProcessStatus
  case object SHUTTING_DOWN extends ProcessStatus

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole

  val SUPPORTED_FEATURES = Collections.
    unmodifiableMap[String, VersionRange](Map[String, VersionRange]().asJava)
}
