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
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.requests.RequestContext;

import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

@InterfaceStability.Evolving
public interface ShareCoordinator {

  short SHARE_SNAPSHOT_RECORD_KEY_VERSION = 0;
  short SHARE_SNAPSHOT_RECORD_VALUE_VERSION = 0;
  short SHARE_UPDATE_RECORD_KEY_VERSION = 1;
  short SHARE_UPDATE_RECORD_VALUE_VERSION = 1;

  /**
   * Return the partition index for the given key.
   *
   * @param key key - groupId:topicId:partitionId.
   * @return The partition index.
   */
  int partitionFor(String key);

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

  /**
   * Handle write share state call
   * @param context
   * @param request
   * @return completable future comprizing of write RPC response data
   */
  CompletableFuture<WriteShareGroupStateResponseData> writeState(RequestContext context, WriteShareGroupStateRequestData request);


  /**
   * Handle read share state call
   * @param context
   * @param request
   * @return completable future comprizing of write RPC response data
   */
  CompletableFuture<ReadShareGroupStateResponseData> readState(RequestContext context, ReadShareGroupStateRequestData request);

  /**
   * Called when new coordinator is elected
   * @param partitionIndex
   * @param partitionLeaderEpoch
   */
  void onElection(int partitionIndex, int partitionLeaderEpoch);

  /**
   * Called when coordinator goes down
   * @param partitionIndex
   * @param partitionLeaderEpoch
   */
  void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch);
}