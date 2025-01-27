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
package org.apache.kafka.coordinator.group.metrics;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GroupCoordinatorMetricsShardTest {

    @Test
    public void testTimelineGaugeCounters() {
        MetricsRegistry registry = new MetricsRegistry();
        Metrics metrics = new Metrics();
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        TopicPartition tp = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);
        GroupCoordinatorMetrics coordinatorMetrics = new GroupCoordinatorMetrics(registry, metrics);
        GroupCoordinatorMetricsShard shard = coordinatorMetrics.newMetricsShard(snapshotRegistry, tp);

        shard.incrementNumOffsets();
        shard.setConsumerGroupGauges(Map.of(
            ConsumerGroup.ConsumerGroupState.EMPTY, 1L,
            ConsumerGroup.ConsumerGroupState.ASSIGNING, 1L,
            ConsumerGroup.ConsumerGroupState.RECONCILING, 1L,
            ConsumerGroup.ConsumerGroupState.STABLE, 1L,
            ConsumerGroup.ConsumerGroupState.DEAD, 1L
        ));

        snapshotRegistry.idempotentCreateSnapshot(1000);
        // The value should not be updated until the offset has been committed.
        assertEquals(0, shard.numOffsets());
        assertEquals(0, shard.numConsumerGroups());
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));

        shard.commitUpTo(1000);
        assertEquals(1, shard.numOffsets());
        assertEquals(5, shard.numConsumerGroups());
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(1, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));

        shard.decrementNumOffsets();
        shard.setConsumerGroupGauges(Map.of(
            ConsumerGroup.ConsumerGroupState.EMPTY, 0L,
            ConsumerGroup.ConsumerGroupState.ASSIGNING, 0L,
            ConsumerGroup.ConsumerGroupState.RECONCILING, 0L,
            ConsumerGroup.ConsumerGroupState.STABLE, 0L,
            ConsumerGroup.ConsumerGroupState.DEAD, 0L
        ));

        snapshotRegistry.idempotentCreateSnapshot(2000);
        shard.commitUpTo(2000);
        assertEquals(0, shard.numOffsets());
        assertEquals(0, shard.numConsumerGroups());
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.EMPTY));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.ASSIGNING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.RECONCILING));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.STABLE));
        assertEquals(0, shard.numConsumerGroups(ConsumerGroup.ConsumerGroupState.DEAD));
    }
}
