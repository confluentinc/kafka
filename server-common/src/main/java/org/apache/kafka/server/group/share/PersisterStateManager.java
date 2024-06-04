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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PersisterStateManager {

  private SendThread sender;
  private static Logger log = LoggerFactory.getLogger(PersisterStateManager.class);
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  public PersisterStateManager(KafkaClient client, Supplier<List<Node>> nodeSupplier, Time time) {
    if (client == null) {
      throw new IllegalArgumentException("Kafkaclient must not be null.");
    }
    if (nodeSupplier == null) {
      throw new IllegalArgumentException("NodeSupplier must not be null.");
    }
    this.sender = new SendThread(
        "PersisterStateManager",
        Objects.requireNonNull(client),
        30_000,  //30 seconds
        time == null ? Time.SYSTEM : time,
        true,
        Objects.requireNonNull(nodeSupplier));
  }

  public void enqueue(PersisterStateManagerHandler handler) {
    this.sender.enqueue(handler);
  }

  public void start() {
    if (!isStarted.get()) {
      this.sender.start();
      isStarted.set(true);
    }
  }

  public void stop() throws InterruptedException {
    if (isStarted.get()) {
      this.sender.shutdown();
    }
  }

  public abstract class PersisterStateManagerHandler implements RequestCompletionHandler {
    protected Node coordinatorNode;
    protected String groupId;
    protected Uuid topicId;
    protected int partition;

    public PersisterStateManagerHandler(String groupId, Uuid topicId, int partition) {
      this.groupId = groupId;
      this.topicId = topicId;
      this.partition = partition;
    }

    /**
     * looks for single group:topic:partition item
     *
     * @return builder for the same
     */
    protected abstract AbstractRequest.Builder<? extends AbstractRequest> requestBuilder();

    /**
     * looks for single group:topic:partition coordinator
     *
     * @return builder for the same
     */
    protected AbstractRequest.Builder<? extends AbstractRequest> findShareCoordinatorBuilder() {
      return new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
          .setKeyType(FindCoordinatorRequest.CoordinatorType.SHARE.id())
          .setKey(coordinatorKey()));
    }

    protected Node shareCoordinatorNode() {
      return coordinatorNode;
    }

    protected boolean lookupNeeded() {
      return coordinatorNode == null;
    }

    protected String coordinatorKey() {
      return ShareGroupHelper.coordinatorKey(groupId, topicId, partition);
    }

    protected boolean isFindCoordinatorResponse(ClientResponse response) {
      return response != null && response.requestHeader().apiKey() == ApiKeys.FIND_COORDINATOR;
    }

    @Override
    public void onComplete(ClientResponse response) {
      if (response != null && response.hasResponse()) {
        if (isFindCoordinatorResponse(response)) {
          handleFindCoordinatorResponse(response);
        } else if (isRequestResponse(response)) {
          handleRequestResponse(response);
        }
      }
    }

    protected void handleFindCoordinatorResponse(ClientResponse response) {
      List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) response.responseBody()).coordinators();
      if (coordinators.size() != 1) {
        log.error("ReadState coordinator response for {} is invalid", coordinatorKey());
        //todo smjn: how to handle?
      }
      FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
      Errors error = Errors.forCode(coordinatorData.errorCode());
      if (error == Errors.NONE) {
        log.info("Find coordinator response received.");
        coordinatorNode = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
        // now we want the read state call to happen
        enqueue(this);  //todo smjn: enable this when read RPC is working
      }
    }

    protected abstract void handleRequestResponse(ClientResponse response);

    protected abstract boolean isRequestResponse(ClientResponse response);
  }

  public class ReadStateHandler extends PersisterStateManagerHandler {
    // result of one group:topicId:partition call
    private CompletableFuture<TopicData<PartitionAllData>> result = new CompletableFuture<>();
    private final int leaderEpoch;

    public ReadStateHandler(String groupId, Uuid topicId, int partition, int leaderEpoch) {
      super(groupId, topicId, partition);
      this.leaderEpoch = leaderEpoch;
    }

    @Override
    protected boolean isRequestResponse(ClientResponse response) {
      return response.requestHeader().apiKey() == ApiKeys.READ_SHARE_GROUP_STATE;
    }

    @Override
    protected void handleRequestResponse(ClientResponse response) {
      log.info("ReadState response");
    }

    @Override
    protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
      return new ReadShareGroupStateRequest.Builder(new ReadShareGroupStateRequestData()
          .setGroupId(groupId)
          .setTopics(Collections.singletonList(
              new ReadShareGroupStateRequestData.ReadStateData()
                  .setTopicId(topicId)
                  .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                      .setPartition(partition)
                      .setLeaderEpoch(leaderEpoch))))));
    }

    public CompletableFuture<TopicData<PartitionAllData>> result() {
      return this.result;
    }
  }

  public class WriteStateHandler extends PersisterStateManagerHandler {
    private final int stateEpoch;
    private final int leaderEpoch;
    private final long startOffset;
    private final List<PersisterStateBatch> batches;

    private final CompletableFuture<WriteShareGroupStateResponse> result;

    public WriteStateHandler(String groupId, Uuid topicId, int partition, int stateEpoch, int leaderEpoch, long startOffset, List<PersisterStateBatch> batches, CompletableFuture<WriteShareGroupStateResponse> result) {
      super(groupId, topicId, partition);
      this.stateEpoch = stateEpoch;
      this.leaderEpoch = leaderEpoch;
      this.startOffset = startOffset;
      this.batches = batches;
      this.result = result;
    }

    @Override
    protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
      return new WriteShareGroupStateRequest.Builder(new WriteShareGroupStateRequestData()
          .setGroupId(groupId)
          .setTopics(Collections.singletonList(
              new WriteShareGroupStateRequestData.WriteStateData()
                  .setTopicId(topicId)
                  .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                      .setPartition(partition)
                      .setStateEpoch(stateEpoch)
                      .setLeaderEpoch(leaderEpoch)
                      .setStartOffset(startOffset)
                      .setStateBatches(batches.stream()
                          .map(batch -> new WriteShareGroupStateRequestData.StateBatch()
                              .setFirstOffset(batch.firstOffset())
                              .setLastOffset(batch.lastOffset())
                              .setDeliveryState(batch.deliveryState())
                              .setDeliveryCount(batch.deliveryCount()))
                          .collect(Collectors.toList())))))));
    }

    @Override
    protected boolean isRequestResponse(ClientResponse response) {
      return response.requestHeader().apiKey() == ApiKeys.WRITE_SHARE_GROUP_STATE;
    }

    @Override
    protected void handleRequestResponse(ClientResponse response) {
      log.info("Write state response received. - {}", response);
      this.result.complete((WriteShareGroupStateResponse) response.responseBody());
    }
  }

  private static class SendThread extends InterBrokerSendThread {
    private final ConcurrentLinkedQueue<PersisterStateManagerHandler> queue = new ConcurrentLinkedQueue<>();
    private final Supplier<List<Node>> nodeSupplier;
    private static final Random RAND = new Random(System.currentTimeMillis());

    public SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible, Supplier<List<Node>> nodeSupplier) {
      super(name, networkClient, requestTimeoutMs, time, isInterruptible);
      this.nodeSupplier = nodeSupplier;
    }

    private Node randomNode() {
      List<Node> nodes = nodeSupplier.get();
      return nodes.get(RAND.nextInt(nodes.size()));
    }

    /**
     * the incoming requests will have the keys in the following format
     * groupId: [
     * topidId1: [part1, part2, part3],
     * topicId2: [part1, part2, part3]
     * ...
     * ]
     * Hence, the total number of keys would be 1 x m x n (1 is for the groupId) where m is number of topicIds
     * and n is number of partitions specified per topicId.
     * Therefore, we must issue a find coordinator RPC for each of the mn keys
     * and then a read state RPC again for each of the mn keys. Hence, resulting in 2mn RPC calls
     * due to 1 request.
     *
     * @return list of requests to manage
     */
    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
      if (!queue.isEmpty()) {
        PersisterStateManagerHandler handler = queue.peek();
        queue.poll();
        if (handler.lookupNeeded()) {
          // we need to find the coordinator node
          return Collections.singletonList(new RequestAndCompletionHandler(
              System.currentTimeMillis(),
              randomNode(),
              handler.findShareCoordinatorBuilder(),
              handler
          ));
        } else {
          // share coord node already available
          return Collections.singletonList(new RequestAndCompletionHandler(
              System.currentTimeMillis(),
              handler.shareCoordinatorNode(),
              handler.requestBuilder(),
              handler
          ));
        }
      }
      return Collections.emptyList();
    }

    public void enqueue(PersisterStateManagerHandler handler) {
      queue.add(handler);
    }
  }
}
