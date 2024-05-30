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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The default implementation of the {@link Persister} interface which is used by the
 * group coordinator and share-partition leaders to manage the durable share-partition state.
 * This implementation uses inter-broker RPCs to make requests with the share coordinator
 * which is responsible for persisting the share-partition state.
 */
@InterfaceStability.Evolving
public class DefaultStatePersister implements Persister {
  private PersisterConfig persisterConfig;
  private PersisterStateManager stateManager;

  private static final Logger log = LoggerFactory.getLogger(DefaultStatePersister.class);

  /**
   * needs to be public as its instance will be
   * created in BrokerServer from class name.
   */
  public DefaultStatePersister() {
  }

  // avoid double check locking - safer, neater
  private static final class InstanceHolder {
    static final Persister INSTANCE = new DefaultStatePersister();
  }

  public static Persister getInstance() {
    return InstanceHolder.INSTANCE;
  }

  @Override
  public void configure(PersisterConfig config) {
    this.persisterConfig = Objects.requireNonNull(config);
    this.stateManager = Objects.requireNonNull(config.stateManager);
  }

  @Override
  public void stop() {
    try {
      this.stateManager.stop();
    } catch (Exception e) {
      log.error("Unable to stop state manager", e);
    }
  }

  /**
   * Used by the group coordinator to initialize the share-partition state.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request InitializeShareGroupStateParameters
   * @return InitializeShareGroupStateResult
   */
  public CompletableFuture<InitializeShareGroupStateResult> initializeState(InitializeShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * Used by share-partition leaders to read share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupStateParameters
   * @return ReadShareGroupStateResult
   */
  public CompletableFuture<ReadShareGroupStateResult> readState(ReadShareGroupStateParameters request) {
    this.stateManager.start();
    GroupTopicPartitionData<PartitionIdLeaderEpochData> gtp = request.groupTopicPartitionData();
    String groupId = gtp.groupId();
    HashMap<Uuid, HashMap<Integer, CompletableFuture<PartitionData>>> futures = new HashMap<>();
    List<PersisterStateManager.ReadStateHandler> handlers = gtp.topicsData().stream()
        .map(topicData -> topicData.partitions().stream()
            .map(partitionData ->
                    {
                        CompletableFuture<PartitionData> future = new CompletableFuture<>();
                        if (futures.containsKey(topicData.topicId())) {
                            futures.get(topicData.topicId()).put(partitionData.partition(), future);
                        } else {
                            HashMap<Integer, CompletableFuture<PartitionData>> map = new HashMap<>();
                            map.put(partitionData.partition(), future);
                            futures.put(topicData.topicId(), map);
                        }
                        return stateManager.new ReadStateHandler(
                                groupId,
                                topicData.topicId(),
                                partitionData.partition(),
                                partitionData.leaderEpoch(),
                                future
                        );
                    }
            )
            .collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

    for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
      stateManager.enqueue(handler);
    }

    // Combine all futures into a single CompletableFuture<Void>
    CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

    // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResult>
    return allOf.thenApply(v -> {
      List<TopicData<PartitionData>> topicDataList = futures.entrySet().stream()
              .map(topicEntry -> {
                Uuid topicId = topicEntry.getKey();
                List<PartitionData> partitionDataList = topicEntry.getValue().values().stream()
                        .map(future -> {
                          try {
                            return future.get();
                          } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                          }
                        })
                        .collect(Collectors.toList());
                return new TopicData<>(topicId, partitionDataList);
              })
              .collect(Collectors.toList());
      return ReadShareGroupStateResult.from(topicDataList);
    });
  }

  /**
   * Used by share-partition leaders to write share-partition state to a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request WriteShareGroupStateParameters
   * @return WriteShareGroupStateResult
   */
  public CompletableFuture<WriteShareGroupStateResult> writeState(WriteShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * Used by the group coordinator to delete share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request DeleteShareGroupStateParameters
   * @return DeleteShareGroupStateResult
   */
  public CompletableFuture<DeleteShareGroupStateResult> deleteState(DeleteShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * Used by the group coordinator to read the offset information from share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupStateSummaryParameters
   * @return ReadShareGroupStateSummaryResult
   */
  public CompletableFuture<ReadShareGroupStateSummaryResult> readSummary(ReadShareGroupStateSummaryParameters request) {
    throw new RuntimeException("not implemented");
  }
}
