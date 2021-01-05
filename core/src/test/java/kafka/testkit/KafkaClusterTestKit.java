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

package kafka.testkit;

import kafka.raft.KafkaRaftManager;
import kafka.raft.RaftManager;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.server.Kip500Broker;
import kafka.server.Kip500Controller;
import kafka.tools.StorageTool;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metalog.LocalLogManager;
import org.apache.kafka.test.TestUtils;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation") // Needed for Scala 2.12 compatibility
public class KafkaClusterTestKit implements AutoCloseable {
    /**
     * This class manages a future which is completed with the proper value for
     * controller.quorum.voters once the randomly assigned ports for all the controllers are
     * known.
     */
    private static class ControllerQuorumVotersFutureManager implements AutoCloseable {
        private final int expectedControllers;
        private final CompletableFuture<String> future = new CompletableFuture<>();
        private final Map<Integer, Integer> controllerPorts = new TreeMap<>();

        ControllerQuorumVotersFutureManager(int expectedControllers) {
            this.expectedControllers = expectedControllers;
        }

        synchronized void registerPort(int nodeId, int port) {
            controllerPorts.put(nodeId, port);
            if (controllerPorts.size() >= expectedControllers) {
                future.complete(controllerPorts.entrySet().stream().
                    map(e -> String.format("%d@localhost:%d", e.getKey(), e.getValue())).
                    collect(Collectors.joining(",")));
            }
        }

        void fail(Throwable e) {
            future.completeExceptionally(e);
        }

        @Override
        public void close() {
            future.cancel(true);
        }
    }

    public static class Builder {
        private TestKitNodes nodes;
        private Map<String, String> configProps = new HashMap<>();

        public Builder(TestKitNodes nodes) {
            this.nodes = nodes;
        }

        public Builder setConfigProp(String key, String value) {
            this.configProps.put(key, value);
            return this;
        }

        public KafkaClusterTestKit build() throws Exception {
            Map<Integer, Kip500Controller> controllers = new HashMap<>();
            Map<Integer, Kip500Broker> kip500Brokers = new HashMap<>();
            ExecutorService executorService = null;
            ControllerQuorumVotersFutureManager connectFutureManager =
                new ControllerQuorumVotersFutureManager(nodes.controllerNodes().size());
            File baseDirectory = null;
            LocalLogManager metaLogManager = null;
            LocalLogManager.SharedLogData sharedLogData = new LocalLogManager.SharedLogData();
            try {
                baseDirectory = TestUtils.tempDirectory();
                nodes = nodes.copyWithAbsolutePaths(baseDirectory.getAbsolutePath());
                executorService = Executors.newFixedThreadPool(
                    nodes.brokerNodes().size() + nodes.controllerNodes().size(),
                    ThreadUtils.createThreadFactory("KafkaClusterTestKit%d", false));
                Time time = Time.SYSTEM;

                // Generate quorum voters connect configuration - Specify controller ports
                Map<Integer, Integer> controllerPorts = new HashMap<>();
                Iterator<Integer> nodeIDs = nodes.controllerNodes().keySet().stream().iterator();
                new Random().ints(10240, 65535)
                        .distinct()
                        .limit(nodes.controllerNodes().size())
                        .forEach(port -> {
                            controllerPorts.put(nodeIDs.next(), port);
                        });
                StringBuilder builder = new StringBuilder();
                String prefix = "";
                for (Map.Entry<Integer, Integer> ports : controllerPorts.entrySet()) {
                    builder.append(prefix).append(ports.getKey()).append('@');
                    builder.append("localhost").append(':').append(ports.getValue());
                    prefix = ",";
                }

                for (ControllerNode node : nodes.controllerNodes().values()) {
                    Map<String, String> props = new HashMap<>(configProps);
                    props.put(KafkaConfig$.MODULE$.ProcessRolesProp(), "controller");
                    props.put(KafkaConfig$.MODULE$.ControllerIdProp(),
                        Integer.toString(node.id()));
                    props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                        node.metadataDirectory());
                    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
                        "CONTROLLER:PLAINTEXT");
                    props.put(KafkaConfig$.MODULE$.ListenersProp(),
                        "CONTROLLER://localhost:" + controllerPorts.get(node.id()));
                    props.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(),
                        "CONTROLLER");
                    setupNodeDirectories(baseDirectory, node.metadataDirectory(),
                        Collections.emptyList());
                    // Note: we can't accurately set controller.quorum.voters yet, since we don't
                    // yet know what ports each controller will pick.  Set it to an
                    // empty string for now as a placeholder.
                    props.put(KafkaConfig$.MODULE$.ControllerQuorumVotersProp(), builder.toString());
                    KafkaConfig config = new KafkaConfig(props, false,
                        OptionConverters.toScala(Optional.empty()));

                    String threadNamePrefix = String.format("controller%d_", node.id());
                    LogContext logContext = new LogContext("[Controller id=" + node.id() + "] ");

                    metaLogManager = new LocalLogManager(
                        logContext,
                        node.id(),
                        sharedLogData,
                        threadNamePrefix
                    );
                    metaLogManager.initialize();
                    MetaProperties properties = MetaProperties.apply(
                        nodes.clusterId(),
                        OptionConverters.toScala(Optional.empty()),
                        OptionConverters.toScala(Optional.of(node.id()))
                    );
                    final int fakeId = 100; //Integer.MAX_VALUE;
                    MetaProperties metaProperties = MetaProperties.apply(Uuid.ZERO_UUID,
                            OptionConverters.toScala(Optional.of(fakeId)),
                            OptionConverters.toScala(Optional.empty()));
                    TopicPartition metadataPartition = new TopicPartition(KafkaServer.metadataTopicName(), 0);
                    KafkaRaftManager raftManager = new KafkaRaftManager(
                            metaProperties,
                            metadataPartition,
                            config,
                            Time.SYSTEM,
                            new Metrics());

                    Kip500Controller controller = new Kip500Controller(
                        nodes.controllerProperties(node.id()),
                        config,
                        metaLogManager,
                        raftManager,
                        time,
                        new Metrics(),
                        OptionConverters.toScala(Optional.of(threadNamePrefix)),
                        connectFutureManager.future
                    );
                    controllers.put(node.id(), controller);
                    controller.socketServerFirstBoundPortFuture().whenComplete((port, e) -> {
                        if (e != null) {
                            connectFutureManager.fail(e);
                        } else {
                            connectFutureManager.registerPort(node.id(), port);
                        }
                    });
                }
                for (Kip500BrokerNode node : nodes.brokerNodes().values()) {
                    Map<String, String> props = new HashMap<>(configProps);
                    props.put(KafkaConfig$.MODULE$.ProcessRolesProp(), "broker");
                    props.put(KafkaConfig$.MODULE$.BrokerIdProp(),
                        Integer.toString(node.id()));
                    props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                        node.metadataDirectory());
                    props.put(KafkaConfig$.MODULE$.LogDirsProp(),
                        String.join(",", node.logDataDirectories()));
                    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
                        "EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
                    props.put(KafkaConfig$.MODULE$.ListenersProp(),
                        "EXTERNAL://localhost:0");
                    props.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
                        nodes.interBrokerListenerName().value());
                    props.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(),
                        "CONTROLLER");

                    setupNodeDirectories(baseDirectory, node.metadataDirectory(),
                        node.logDataDirectories());

                    // Just like above, we set a placeholder voter list here until we
                    //find out what ports the controllers picked.
                    props.put(KafkaConfig$.MODULE$.ControllerQuorumVotersProp(), "");
                    KafkaConfig config = new KafkaConfig(props, false,
                        OptionConverters.toScala(Optional.empty()));

                    String threadNamePrefix = String.format("broker%d_", node.id());
                    LogContext logContext = new LogContext("[Broker id=" + node.id() + "] ");

                    metaLogManager = new LocalLogManager(
                        logContext,
                        node.id(),
                        sharedLogData,
                        threadNamePrefix
                    );
                    metaLogManager.initialize();
                    Kip500Broker broker = new Kip500Broker(
                        config,
                        nodes.brokerProperties(node.id()),
                        metaLogManager,
                        time,
                        new Metrics(),
                        OptionConverters.toScala(Optional.of(threadNamePrefix)),
                        JavaConverters.asScalaBuffer(Collections.<String>emptyList()).toSeq(),
                        connectFutureManager.future,
                        KafkaServer.SUPPORTED_FEATURES()
                    );
                    kip500Brokers.put(node.id(), broker);
                }
            } catch (Exception e) {
                if (executorService != null) {
                    executorService.shutdownNow();
                    executorService.awaitTermination(1, TimeUnit.DAYS);
                }
                for (Kip500Controller controller : controllers.values()) {
                    controller.shutdown();
                }
                for (Kip500Broker kip500Broker : kip500Brokers.values()) {
                    kip500Broker.shutdown();
                }
                if (metaLogManager != null) {
                    metaLogManager.close();
                }
                connectFutureManager.close();
                if (baseDirectory != null) {
                    Utils.delete(baseDirectory);
                }
                throw e;
            }
            return new KafkaClusterTestKit(executorService, nodes, controllers,
                kip500Brokers, connectFutureManager, baseDirectory);
        }

        static private void setupNodeDirectories(File baseDirectory,
                                                 String metadataDirectory,
                                                 Collection<String> logDataDirectories) throws Exception {
            Files.createDirectories(new File(baseDirectory, "local").toPath());
            Files.createDirectories(Paths.get(metadataDirectory));
            for (String logDataDirectory : logDataDirectories) {
                Files.createDirectories(Paths.get(logDataDirectory));
            }
        }
    }

    static class MockRaftManager implements RaftManager {
        @Override
        public CompletableFuture<ApiMessage> handleRequest(RequestHeader header, ApiMessage data) {
            CompletableFuture<ApiMessage> future = new CompletableFuture<>();
            future.completeExceptionally(
                new UnsupportedVersionException("API " + header.apiKey() + " is not available"));
            return future;
        }
    }

    private final ExecutorService executorService;
    private final TestKitNodes nodes;
    private final Map<Integer, Kip500Controller> controllers;
    private final Map<Integer, Kip500Broker> kip500Brokers;
    private final ControllerQuorumVotersFutureManager controllerQuorumVotersFutureManager;
    private final File baseDirectory;

    private KafkaClusterTestKit(ExecutorService executorService,
                                TestKitNodes nodes,
                                Map<Integer, Kip500Controller> controllers,
                                Map<Integer, Kip500Broker> kip500Brokers,
                                ControllerQuorumVotersFutureManager controllerQuorumVotersFutureManager,
                                File baseDirectory) {
        this.executorService = executorService;
        this.nodes = nodes;
        this.controllers = controllers;
        this.kip500Brokers = kip500Brokers;
        this.controllerQuorumVotersFutureManager = controllerQuorumVotersFutureManager;
        this.baseDirectory = baseDirectory;
    }

    public void format() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Entry<Integer, Kip500Controller> entry : controllers.entrySet()) {
                int nodeId = entry.getKey();
                Kip500Controller controller = entry.getValue();
                futures.add(executorService.submit(() -> {
                    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                        try (PrintStream out = new PrintStream(stream)) {
                            StorageTool.formatCommand(out,
                                JavaConverters.asScalaBuffer(Collections.singletonList(
                                    controller.config().metadataLogDir())).toSeq(),
                                nodes.controllerProperties(nodeId),
                                false);
                        } finally {
                            for (String line : stream.toString().split(String.format("%n"))) {
                                controller.info(() -> line);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Entry<Integer, Kip500Broker> entry : kip500Brokers.entrySet()) {
                int nodeId = entry.getKey();
                Kip500Broker broker = entry.getValue();
                futures.add(executorService.submit(() -> {
                    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                        try (PrintStream out = new PrintStream(stream)) {
                            StorageTool.formatCommand(out,
                                JavaConverters.asScalaBuffer(Collections.singletonList(
                                    broker.config().metadataLogDir())).toSeq(),
                                nodes.brokerProperties(nodeId),
                                false);
                        } finally {
                            for (String line : stream.toString().split(String.format("%n"))) {
                                broker.info(() -> line);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    public void startup() throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (KafkaRaftManager raftManager : raftManagers.values()) {
                futures.add(executorService.submit(raftManager::startup));
            }
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    controller.startup();
                }));
            }
            for (Kip500Broker broker : kip500Brokers.values()) {
                futures.add(executorService.submit(broker::startup));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    public Properties controllerClientProperties() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        if (!controllers.isEmpty()) {
            Collection<Node> controllerNodes = JavaConverters.asJavaCollection(
                KafkaConfig$.MODULE$.controllerQuorumVoterStringsToNodes(
                    controllerQuorumVotersFutureManager.future.get()));
            StringBuilder bld = new StringBuilder();
            String prefix = "";
            for (Node node : controllerNodes) {
                bld.append(prefix).append(node.id()).append('@');
                bld.append(node.host()).append(":").append(node.port());
                prefix = ",";
            }
            properties.setProperty(KafkaConfig$.MODULE$.ControllerQuorumVotersProp(), bld.toString());
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                controllerNodes.stream().map(n -> n.host() + ":" + n.port()).
                    collect(Collectors.joining(",")));
        }
        return properties;
    }

    public Properties clientProperties() {
        Properties properties = new Properties();
        if (!kip500Brokers.isEmpty()) {
            StringBuilder bld = new StringBuilder();
            String prefix = "";
            for (Entry<Integer, Kip500Broker> entry : kip500Brokers.entrySet()) {
                int brokerId = entry.getKey();
                Kip500Broker broker = entry.getValue();
                ListenerName listenerName = nodes.externalListenerName();
                int port = broker.boundPort(listenerName);
                if (port <= 0) {
                    throw new RuntimeException("Broker " + brokerId + " does not yet " +
                        "have a bound port for " + listenerName + ".  Did you start " +
                        "the cluster yet?");
                }
                bld.append(prefix).append("localhost:").append(port);
                prefix = ",";
            }
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bld.toString());
        }
        return properties;
    }

    public Map<Integer, Kip500Controller> controllers() {
        return controllers;
    }

    public Map<Integer, Kip500Broker> kip500Brokers() {
        return kip500Brokers;
    }

    public TestKitNodes nodes() {
        return nodes;
    }

    @Override
    public void close() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            controllerQuorumVotersFutureManager.close();
            for (Kip500Broker kip500Broker : kip500Brokers.values()) {
                futures.add(executorService.submit(kip500Broker::shutdown));
            }
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(controller::shutdown));
            }
            for (KafkaRaftManager raftManager : raftManagers.values()) {
                futures.add(executorService.submit(raftManager::shutdown));
            }
            for (Future<?> future: futures) {
                future.get();
            }
            Utils.delete(baseDirectory);
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
    }
}
