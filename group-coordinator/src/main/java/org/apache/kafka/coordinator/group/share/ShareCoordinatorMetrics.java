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

package org.apache.kafka.coordinator.group.share;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Objects;

// todo smjn- need to add metrics. Currently placeholder for adhering to coordinator API
public class ShareCoordinatorMetrics extends CoordinatorMetrics implements AutoCloseable {

  private final MetricsRegistry registry;
  private final Metrics metrics;

  public ShareCoordinatorMetrics() {
    this(KafkaYammerMetrics.defaultRegistry(), new Metrics());
  }

  public ShareCoordinatorMetrics(MetricsRegistry registry, Metrics metrics) {
    this.registry = Objects.requireNonNull(registry);
    this.metrics = Objects.requireNonNull(metrics);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public ShareCoordinatorMetricsShard newMetricsShard(SnapshotRegistry snapshotRegistry, TopicPartition tp) {
    return null;
  }

  @Override
  public void activateMetricsShard(CoordinatorMetricsShard shard) {

  }

  @Override
  public void deactivateMetricsShard(CoordinatorMetricsShard shard) {

  }

  @Override
  public MetricsRegistry registry() {
    return null;
  }

  @Override
  public void onUpdateLastCommittedOffset(TopicPartition tp, long offset) {

  }
}
