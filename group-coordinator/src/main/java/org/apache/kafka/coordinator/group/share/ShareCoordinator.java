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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.RequestContext;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

@InterfaceStability.Evolving
public interface ShareCoordinator {

  /**
   * Return the partition index for the given key.
   *
   * @param key key - groupId:topicId:partitionId.
   * @return The partition index.
   */
  int partitionFor(String key);

  CompletableFuture<ReadShareGroupStateResponseData> readShareGroupStates(
          RequestContext context,
          ReadShareGroupStateRequestData requestData
  );

  /**
   * Read Share Group State.
   *
   * @param context           The coordinator request context.
   * @param requestPartition  The ReadShareGroupStateRequest.Partition info.
   *
   * @return A future yielding the results or an exception.
   */
  CompletableFuture<ReadShareGroupStateResponseData.PartitionResult> readShareGroupState(
          RequestContext context,
          String groupId,
          String topicId,
          ReadShareGroupStateRequestData.PartitionData requestPartition
  );

  /**
   * Return the configuration properties of the share-group state topic.
   *
   * @return Properties of the share-group state topic.
   */
  Properties shareGroupStateTopicConfigs();

  /**
   * Start the share coordinator
   *
   * @param shareGroupTopicPartitionCount - supplier returning the number of partitions for __share_group_state topic
   */
  void startup(IntSupplier shareGroupTopicPartitionCount);

  /**
   * Stop the share coordinator
   */
  void shutdown();
}
