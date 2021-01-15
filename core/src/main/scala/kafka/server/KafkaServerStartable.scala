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

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}
import org.apache.kafka.common.utils.Time

object KafkaServerStartable {
  def fromProps(serverProps: Properties): KafkaServerStartable = {
    fromProps(serverProps, None)
  }

  def fromProps(serverProps: Properties, threadNamePrefix: Option[String]): KafkaServerStartable = {
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters.toSeq, threadNamePrefix)
  }
}

class KafkaServerStartable(
  val staticServerConfig: KafkaConfig,
  reporters: Seq[KafkaMetricsReporter],
  threadNamePrefix: Option[String] = None
) extends Logging {
  private val server = KafkaServer(staticServerConfig, Time.SYSTEM, threadNamePrefix, reporters)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  def startup(): Unit = {
    try server.startup()
    catch {
      case e: Throwable =>
        // KafkaServerStartable.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal(s"Exiting Kafka - ${e.getMessage}" )
        Exit.exit(1)
    }
  }

  def shutdown(): Unit = {
    try {
      server.shutdown()
    } catch {
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  def awaitShutdown(): Unit = server.awaitShutdown()
}


