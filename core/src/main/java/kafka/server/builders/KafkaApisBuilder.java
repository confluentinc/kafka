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

package kafka.server.builders;

import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.network.RequestChannel;
import kafka.server.AutoTopicCreationManager;
import kafka.server.FetchManager;
import kafka.server.ForwardingManager;
import kafka.server.KafkaApis;
import kafka.server.KafkaConfig;
import kafka.server.QuotaFactory.QuotaManagers;
import kafka.server.ReplicaManager;
import kafka.server.share.SharePartitionManager;

import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinator;
import org.apache.kafka.coordinator.share.ShareCoordinator;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.ApiVersionManager;
import org.apache.kafka.server.ClientMetricsManager;
import org.apache.kafka.server.DelegationTokenManager;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import scala.jdk.javaapi.OptionConverters;

public class KafkaApisBuilder {
    private RequestChannel requestChannel = null;
    private ForwardingManager forwardingManager = null;
    private ReplicaManager replicaManager = null;
    private GroupCoordinator groupCoordinator = null;
    private TransactionCoordinator txnCoordinator = null;
    private AutoTopicCreationManager autoTopicCreationManager = null;
    private int brokerId = 0;
    private KafkaConfig config = null;
    private ConfigRepository configRepository = null;
    private MetadataCache metadataCache = null;
    private Metrics metrics = null;
    private Optional<Plugin<Authorizer>> authorizerPlugin = Optional.empty();
    private QuotaManagers quotas = null;
    private FetchManager fetchManager = null;
    private SharePartitionManager sharePartitionManager = null;
    private BrokerTopicStats brokerTopicStats = null;
    private String clusterId = "clusterId";
    private Time time = Time.SYSTEM;
    private DelegationTokenManager tokenManager = null;
    private ApiVersionManager apiVersionManager = null;
    private ClientMetricsManager clientMetricsManager = null;
    private ShareCoordinator shareCoordinator = null;
    private GroupConfigManager groupConfigManager = null;
    private Supplier<Long> brokerEpochSupplier = () -> -1L;

    public KafkaApisBuilder setRequestChannel(RequestChannel requestChannel) {
        this.requestChannel = requestChannel;
        return this;
    }

    public KafkaApisBuilder setForwardingManager(ForwardingManager forwardingManager) {
        this.forwardingManager = forwardingManager;
        return this;
    }

    public KafkaApisBuilder setReplicaManager(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
        return this;
    }

    public KafkaApisBuilder setGroupCoordinator(GroupCoordinator groupCoordinator) {
        this.groupCoordinator = groupCoordinator;
        return this;
    }

    public KafkaApisBuilder setTxnCoordinator(TransactionCoordinator txnCoordinator) {
        this.txnCoordinator = txnCoordinator;
        return this;
    }

    public KafkaApisBuilder setShareCoordinator(ShareCoordinator shareCoordinator) {
        this.shareCoordinator = shareCoordinator;
        return this;
    }

    public KafkaApisBuilder setAutoTopicCreationManager(AutoTopicCreationManager autoTopicCreationManager) {
        this.autoTopicCreationManager = autoTopicCreationManager;
        return this;
    }

    public KafkaApisBuilder setBrokerId(int brokerId) {
        this.brokerId = brokerId;
        return this;
    }

    public KafkaApisBuilder setConfig(KafkaConfig config) {
        this.config = config;
        return this;
    }

    public KafkaApisBuilder setConfigRepository(ConfigRepository configRepository) {
        this.configRepository = configRepository;
        return this;
    }

    public KafkaApisBuilder setMetadataCache(MetadataCache metadataCache) {
        this.metadataCache = metadataCache;
        return this;
    }

    public KafkaApisBuilder setMetrics(Metrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public KafkaApisBuilder setAuthorizerPlugin(Optional<Plugin<Authorizer>> authorizerPlugin) {
        this.authorizerPlugin = authorizerPlugin;
        return this;
    }

    public KafkaApisBuilder setQuotas(QuotaManagers quotas) {
        this.quotas = quotas;
        return this;
    }

    public KafkaApisBuilder setFetchManager(FetchManager fetchManager) {
        this.fetchManager = fetchManager;
        return this;
    }

    public KafkaApisBuilder setSharePartitionManager(SharePartitionManager sharePartitionManager) {
        this.sharePartitionManager = sharePartitionManager;
        return this;
    }

    public KafkaApisBuilder setBrokerTopicStats(BrokerTopicStats brokerTopicStats) {
        this.brokerTopicStats = brokerTopicStats;
        return this;
    }

    public KafkaApisBuilder setClusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public KafkaApisBuilder setTime(Time time) {
        this.time = time;
        return this;
    }

    public KafkaApisBuilder setTokenManager(DelegationTokenManager tokenManager) {
        this.tokenManager = tokenManager;
        return this;
    }

    public KafkaApisBuilder setApiVersionManager(ApiVersionManager apiVersionManager) {
        this.apiVersionManager = apiVersionManager;
        return this;
    }

    public KafkaApisBuilder setClientMetricsManager(ClientMetricsManager clientMetricsManager) {
        this.clientMetricsManager = clientMetricsManager;
        return this;
    }

    public KafkaApisBuilder setGroupConfigManager(GroupConfigManager groupConfigManager) {
        this.groupConfigManager = groupConfigManager;
        return this;
    }

    public KafkaApisBuilder setBrokerEpochSupplier(Supplier<Long> brokerEpochSupplier) {
        this.brokerEpochSupplier = brokerEpochSupplier;
        return this;
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    public KafkaApis build() {
        if (requestChannel == null) throw new RuntimeException("you must set requestChannel");
        if (forwardingManager == null) throw new RuntimeException("you must set forwardingManager");
        if (replicaManager == null) throw new RuntimeException("You must set replicaManager");
        if (groupCoordinator == null) throw new RuntimeException("You must set groupCoordinator");
        if (txnCoordinator == null) throw new RuntimeException("You must set txnCoordinator");
        if (shareCoordinator == null) throw new RuntimeException("You must set shareCoordinator");
        if (autoTopicCreationManager == null) throw new RuntimeException("You must set autoTopicCreationManager");
        if (config == null) config = new KafkaConfig(Map.of());
        if (configRepository == null) throw new RuntimeException("You must set configRepository");
        if (metadataCache == null) throw new RuntimeException("You must set metadataCache");
        if (metrics == null) throw new RuntimeException("You must set metrics");
        if (quotas == null) throw new RuntimeException("You must set quotas");
        if (fetchManager == null) throw new RuntimeException("You must set fetchManager");
        if (sharePartitionManager == null) throw new RuntimeException("You must set sharePartitionManager");
        if (clientMetricsManager == null) throw new RuntimeException("You must set clientMetricsManager");
        if (brokerTopicStats == null) brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig().isRemoteStorageSystemEnabled());
        if (apiVersionManager == null) throw new RuntimeException("You must set apiVersionManager");
        if (groupConfigManager == null) throw new RuntimeException("You must set groupConfigManager");

        return new KafkaApis(requestChannel,
                             forwardingManager,
                             replicaManager,
                             groupCoordinator,
                             txnCoordinator,
                             shareCoordinator,
                             autoTopicCreationManager,
                             brokerId,
                             config,
                             configRepository,
                             metadataCache,
                             metrics,
                             OptionConverters.toScala(authorizerPlugin),
                             quotas,
                             fetchManager,
                             sharePartitionManager,
                             brokerTopicStats,
                             clusterId,
                             time,
                             tokenManager,
                             apiVersionManager,
                             clientMetricsManager,
                             groupConfigManager,
                             brokerEpochSupplier);
    }
}
