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

package kafka.tools

import joptsimple.{OptionException, OptionSet, OptionSpec}
import kafka.admin.ConfigCommand
import kafka.tools.ClientMetrics.ConfigCommandParser
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.store
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, AlterConfigsOptions, ConfigEntry, DescribeConfigsOptions}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.utils.Utils

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.compat.java8.FunctionConverters.enrichAsJavaFunction
import scala.jdk.CollectionConverters.{IterableHasAsJava, MapHasAsJava}

object ClientMetrics extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandParser(args)
      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to manipulate and describe entity config for a topic, client, user, broker or ip")
      opts.checkArgs()

      processCommand(opts)
    } catch {
      case e@(_: IllegalArgumentException | _: InvalidConfigurationException | _: OptionException) =>
        logger.debug(s"Failed config command with args '${args.mkString(" ")}'", e)
        System.err.println(e.getMessage)
        Exit.exit(1)

      case t: Throwable =>
        logger.debug(s"Error while executing config command with args '${args.mkString(" ")}'", t)
        System.err.println(s"Error while executing config command with args '${args.mkString(" ")}'")
        t.printStackTrace(System.err)
        Exit.exit(1)
    }
  }

  private def processCommand(opts: ConfigCommandParser): Unit = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    val adminClient = Admin.create(props)

    try {
      if (opts.options.has(opts.addOp) || opts.options.has(opts.deleteOp))
        alterConfig(adminClient, opts)
      else if (opts.options.has(opts.listOp))
        describeConfig(adminClient, opts)
    } finally {
      adminClient.close()
    }
  }

  def alterConfig(adminClient: Admin, opts: ConfigCommandParser): Unit = {

  }

  def describeConfig(adminClient: Admin, opts: ConfigCommandParser) = {

  }

  class ConfigCommandParser(args: Array[String]) extends CommandDefaultOptions(args) {
    val addOp = parser.accepts("add", "Adding telemetry metrics")
    val deleteOp = parser.accepts("delete", "Delete metrics")
    val listOp = parser.accepts("list", "List metrics")

    val matchId = parser.accepts("match", "matching the given client instance")
      .withRequiredArg
      .describedAs("matching a specific client ID")
      .ofType(classOf[String])
    val metric = parser.accepts("metric", "metric prefixes")
      .withRequiredArg
      .ofType(classOf[String])
    val interval = parser.accepts("interval", "push interval ms")
      .withRequiredArg
      .ofType(classOf[Long])
    val block = parser.accepts("block", "blocking metrics collection")
    val name = parser.accepts("name", "metric name")
      .withRequiredArg()
      .describedAs("config name")
      .ofType(classOf[String])

    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka server to connect to. " +
      "This is required for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])

    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])

    options = parser.parse(args : _*)

    def checkArgs(): Unit = {
      // only perform 1 action at a time
      val actions = Seq(addOp, deleteOp).count(options.has _)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --add, --delete, --list")

      // check add args
      CommandLineUtils.checkInvalidArgs(parser, options, addOp, Set(deleteOp, listOp))

      // check delete args
      CommandLineUtils.checkInvalidArgs(parser, options, deleteOp, Set(addOp, listOp, matchId, metric, interval))

      // check list args
      CommandLineUtils.checkInvalidArgs(parser, options, listOp, Set(addOp, listOp, matchId, metric, interval, block))

      if (!options.has(bootstrapServerOpt))
        throw new IllegalArgumentException("--bootstrap-server must be specified")
    }
  }

  object ClientMetricService {
    def ClientMetricService(commandConfig: Properties, bootstrapServer: Option[String]): Admin = {
      bootstrapServer match {
        case Some(serverList) => commandConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverList)
        case None =>
      }
      Admin.create(commandConfig)
    }

    case class ClientMetricService private (adminClient: Admin) extends AutoCloseable {
      def listMetrics(listMetricsOpts: ListMetricsOptions): Unit = {
        val configResource = if(listMetricsOpts.name.isEmpty)
          new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "")
        else
          new ConfigResource(ConfigResource.Type.CLIENT_METRICS, listMetricsOpts.name.get)

        adminClient.describeConfigs(util.Collections.singleton(ConfigResource))
      }

      def deleteMetrics(deleteMetricsOpts: DeleteMetricsOptions): Unit = {
        if(deleteMetricsOpts.name.isEmpty)
          throw new IllegalArgumentException(s"The delete metrics operation requires a name")

        val configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, deleteMetricsOpts.name.get)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        val alterEntries = List(new AlterConfigOp(new ConfigEntry("name", deleteMetricsOpts.name.get), AlterConfigOp.OpType.DELETE))
        adminClient.incrementalAlterConfigs(
          Map(configResource -> alterEntries.asJavaCollection).asJava,
          alterOptions).all().get(60, TimeUnit.SECONDS)
      }
      override def close(): Unit = adminClient.close()
    }

    def addMetrics(addMetricsOpts: AddMetricsOptions): Unit = {
    }
  }

  class ListMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)
  }

  class DeleteMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)
  }

  class AddMetricsOptions(args: ConfigCommandParser) extends MetricsOptions(args.options: OptionSet) {
    def name: Option[String] = valueAsOption(args.name)
    def matchClientId: Option[String] = valueAsOption(args.matchId)
    def metrics: Option[util.List[String]] = valuesAsOption(args.metric)
    def intervalMs: Option[Long] = valueAsOption(args.interval)
    def isBlocked: Boolean = has(args.block)
  }

  case class MetricsOptions(options: OptionSet) {
    def has(builder: OptionSpec[_]): Boolean = options.has(builder)
    def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue
    def valuesAsOption[A](option: OptionSpec[A], defaultValue: Option[util.List[A]] = None): Option[util.List[A]] = if (has(option)) Some(options.valuesOf(option)) else defaultValue
  }
}
