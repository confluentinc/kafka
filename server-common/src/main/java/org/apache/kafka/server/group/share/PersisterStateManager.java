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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class PersisterStateManager {

  private SendThread sender;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private final ShareCoordinatorMetadataCacheHelper cacheHelper;
  public static final long REQUEST_BACKOFF_MS = 1_000L;
  public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;
  private static final int MAX_FIND_COORD_ATTEMPTS = 5;
  private final Time time;


  public PersisterStateManager(KafkaClient client, Time time, ShareCoordinatorMetadataCacheHelper cacheHelper) {
    if (client == null) {
      throw new IllegalArgumentException("Kafkaclient must not be null.");
    }
    if (cacheHelper == null) {
      throw new IllegalArgumentException("ShareCoordinatorMetadataCacheHelper must not be null.");
    }
    this.cacheHelper = cacheHelper;
    this.time = time == null ? Time.SYSTEM : time;
    this.sender = new SendThread(
        "PersisterStateManager",
        client,
        30_000,  //30 seconds
        this.time,
        true,
        new Random(this.time.milliseconds()));
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
    protected final String groupId;
    protected final Uuid topicId;
    protected final int partition;
    private final ExponentialBackoff findCoordbackoff;
    private int findCoordattempts = 0;
    private final int maxFindCoordAttempts;
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public PersisterStateManagerHandler(
        String groupId,
        Uuid topicId,
        int partition,
        long backoffMs,
        long backoffMaxMs,
        int maxFindCoordAttempts
    ) {
      this.groupId = groupId;
      this.topicId = topicId;
      this.partition = partition;
      this.findCoordbackoff = new ExponentialBackoff(
          backoffMs,
          CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
          backoffMaxMs,
          CommonClientConfigs.RETRY_BACKOFF_JITTER);
      this.maxFindCoordAttempts = maxFindCoordAttempts;
    }

    /**
     * Child class must create appropriate builder object for the handled RPC
     *
     * @return builder for the request
     */
    protected abstract AbstractRequest.Builder<? extends AbstractRequest> requestBuilder();

    /**
     * Handles the response for an RPC.
     *
     * @param response - Client response
     */
    protected abstract void handleRequestResponse(ClientResponse response);

    /**
     * Returns true if the response if valid for the respective child class.
     *
     * @param response - Client response
     * @return - boolean
     */
    protected abstract boolean isRequestResponse(ClientResponse response);

    /**
     * Handle invalid find coordinator response. If error is UNKNOWN_SERVER_ERROR. Look at the
     * exception details to figure out the problem.
     *
     * @param error
     * @param exception
     */
    protected abstract void findCoordinatorErrorResponse(Errors error, Exception exception);

    /**
     * Child class must provide a descriptive name for the implementation.
     *
     * @return String
     */
    protected abstract String name();

    /**
     * Returns builder for share coordinator
     *
     * @return builder for find coordinator
     */
    protected AbstractRequest.Builder<? extends AbstractRequest> findShareCoordinatorBuilder() {
      return new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
          .setKeyType(FindCoordinatorRequest.CoordinatorType.SHARE.id())
          .setKey(coordinatorKey()));
    }

    /**
     * Return the share coordinator node
     *
     * @return Node
     */
    protected Node shareCoordinatorNode() {
      return coordinatorNode;
    }

    /**
     * Returns true is coordinator node is not yet set
     *
     * @return boolean
     */
    protected boolean lookupNeeded() {
      if (coordinatorNode != null) {
        return false;
      }
      if (cacheHelper.containsTopic(Topic.SHARE_GROUP_STATE_TOPIC_NAME)) {
        log.info("{} internal topic already exists.", Topic.SHARE_GROUP_STATE_TOPIC_NAME);
        Node node = cacheHelper.getShareCoordinator(coordinatorKey(), Topic.SHARE_GROUP_STATE_TOPIC_NAME);
        if (node != Node.noNode()) {
          log.info("Found coordinator node in cache: {}", node);
          coordinatorNode = node;
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the String key to be used as share coordinator key
     *
     * @return String
     */
    protected String coordinatorKey() {
      return ShareGroupHelper.coordinatorKey(groupId, topicId, partition);
    }

    /**
     * Returns true if the RPC response if for Find Coordinator RPC.
     *
     * @param response - Client response object
     * @return boolean
     */
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

    private void resetAttempts() {
      findCoordattempts = 0;
    }

    private void resetCoordinatorNode() {
      coordinatorNode = null;
    }

    /**
     * Handles the response for find coordinator RPC and sets appropriate state.
     *
     * @param response - Client response for find coordinator RPC
     */
    protected void handleFindCoordinatorResponse(ClientResponse response) {
      log.info("Find coordinator response received.");
      // Incrementing the number of find coordinator attempts
      findCoordattempts++;
      List<FindCoordinatorResponseData.Coordinator> coordinators = ((FindCoordinatorResponse) response.responseBody()).coordinators();
      if (coordinators.size() != 1) {
        log.error("Find coordinator response for {} is invalid", coordinatorKey());
        findCoordinatorErrorResponse(Errors.UNKNOWN_SERVER_ERROR, new IllegalStateException("Invalid response with multiple coordinators."));
        return;
      }

      FindCoordinatorResponseData.Coordinator coordinatorData = coordinators.get(0);
      Errors error = Errors.forCode(coordinatorData.errorCode());

      switch (error) {
        case NONE:
          log.info("Find coordinator response valid. Enqueuing actual request.");
          resetAttempts();
          coordinatorNode = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
          // now we want the actual share state RPC call to happen
          enqueue(this);
          break;

        case COORDINATOR_NOT_AVAILABLE: // retryable error codes
        case COORDINATOR_LOAD_IN_PROGRESS:
          log.warn("Received retryable error in find coordinator {}", error.message());
          if (findCoordattempts >= this.maxFindCoordAttempts) {
            log.error("Exhausted max retries to find coordinator without success.");
            findCoordinatorErrorResponse(error, new Exception("Exhausted max retries to find coordinator without success."));
            break;
          }
          log.info("Waiting before retrying find coordinator RPC.");
          try {
            TimeUnit.MILLISECONDS.sleep(findCoordbackoff.backoff(findCoordattempts));
          } catch (InterruptedException e) {
            log.warn("Interrupted waiting before retrying find coordinator request", e);
          }
          resetCoordinatorNode();
          enqueue(this);
          break;

        default:
          log.error("Unable to find coordinator.");
          findCoordinatorErrorResponse(error, null);
      }
    }

    // Visible for testing
    public Node getCoordinatorNode() {
      return coordinatorNode;
    }
  }

  public class WriteStateHandler extends PersisterStateManagerHandler {
    private final int stateEpoch;
    private final int leaderEpoch;
    private final long startOffset;
    private final List<PersisterStateBatch> batches;
    private final CompletableFuture<WriteShareGroupStateResponse> result;

    public WriteStateHandler(
        String groupId,
        Uuid topicId,
        int partition,
        int stateEpoch,
        int leaderEpoch,
        long startOffset,
        List<PersisterStateBatch> batches,
        CompletableFuture<WriteShareGroupStateResponse> result,
        long backoffMs,
        long backoffMaxMs,
        int maxFindCoordAttempts
    ) {
      super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
      this.stateEpoch = stateEpoch;
      this.leaderEpoch = leaderEpoch;
      this.startOffset = startOffset;
      this.batches = batches;
      this.result = result;
    }

    public WriteStateHandler(
        String groupId,
        Uuid topicId,
        int partition,
        int stateEpoch,
        int leaderEpoch,
        long startOffset,
        List<PersisterStateBatch> batches,
        CompletableFuture<WriteShareGroupStateResponse> result
    ) {
      this(
          groupId,
          topicId,
          partition,
          stateEpoch,
          leaderEpoch,
          startOffset,
          batches,
          result,
          REQUEST_BACKOFF_MS,
          REQUEST_BACKOFF_MAX_MS,
          MAX_FIND_COORD_ATTEMPTS
      );
    }

    @Override
    protected String name() {
      return "WriteStateHandler";
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

    @Override
    protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
      this.result.complete(new WriteShareGroupStateResponse(
          WriteShareGroupStateResponse.toErrorResponseData(topicId, partition, error, "Error in find coordinator. " +
              (exception == null ? error.message() : exception.getMessage()))));
    }

    // Visible for testing
    public CompletableFuture<WriteShareGroupStateResponse> getResult() {
      return result;
    }
  }

  public class ReadStateHandler extends PersisterStateManagerHandler {
    private final int leaderEpoch;
    private final String coordinatorKey;
    private final CompletableFuture<ReadShareGroupStateResponse> result;

    public ReadStateHandler(
        String groupId,
        Uuid topicId,
        int partition,
        int leaderEpoch,
        CompletableFuture<ReadShareGroupStateResponse> result,
        long backoffMs,
        long backoffMaxMs,
        int maxFindCoordAttempts
    ) {
      super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
      this.leaderEpoch = leaderEpoch;
      this.coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);
      this.result = result;
    }

    public ReadStateHandler(
        String groupId,
        Uuid topicId,
        int partition,
        int leaderEpoch,
        CompletableFuture<ReadShareGroupStateResponse> result
    ) {
      this(
          groupId,
          topicId,
          partition,
          leaderEpoch,
          result,
          REQUEST_BACKOFF_MS,
          REQUEST_BACKOFF_MAX_MS,
          MAX_FIND_COORD_ATTEMPTS
      );
    }

    @Override
    protected String name() {
      return "ReadStateHandler";
    }

    @Override
    protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
      return new ReadShareGroupStateRequest.Builder(new ReadShareGroupStateRequestData()
          .setGroupId(groupId)
          .setTopics(Collections.singletonList(
              new ReadShareGroupStateRequestData.ReadStateData()
                  .setTopicId(topicId)
                  .setPartitions(Collections.singletonList(
                      new ReadShareGroupStateRequestData.PartitionData()
                          .setPartition(partition)
                          .setLeaderEpoch(leaderEpoch))))));
    }

    @Override
    protected boolean isRequestResponse(ClientResponse response) {
      return response.requestHeader().apiKey() == ApiKeys.READ_SHARE_GROUP_STATE;
    }

    @Override
    protected void handleRequestResponse(ClientResponse response) {
      log.info("Read state response received. - {}", response);
      ReadShareGroupStateResponseData readShareGroupStateResponseData = ((ReadShareGroupStateResponse) response.responseBody()).data();
      String errorMessage = "Failed to read state for partition " + partition + " in topic " + topicId + " for group " + groupId;
      if (readShareGroupStateResponseData.results().size() != 1) {
        log.error("ReadState response for {} is invalid", coordinatorKey);
        this.result.complete(new ReadShareGroupStateResponse(
            ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
      }
      ReadShareGroupStateResponseData.ReadStateResult topicData = readShareGroupStateResponseData.results().get(0);
      if (!topicData.topicId().equals(topicId)) {
        log.error("ReadState response for {} is invalid", coordinatorKey);
        this.result.complete(new ReadShareGroupStateResponse(
            ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
      }
      if (topicData.partitions().size() != 1) {
        log.error("ReadState response for {} is invalid", coordinatorKey);
        this.result.complete(new ReadShareGroupStateResponse(
            ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
      }
      ReadShareGroupStateResponseData.PartitionResult partitionResponse = topicData.partitions().get(0);
      if (partitionResponse.partition() != partition) {
        log.error("ReadState response for {} is invalid", coordinatorKey);
        this.result.complete(new ReadShareGroupStateResponse(
            ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, Errors.forException(new IllegalStateException(errorMessage)), errorMessage)));
      }
      result.complete((ReadShareGroupStateResponse) response.responseBody());
    }

    @Override
    protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
      this.result.complete(new ReadShareGroupStateResponse(
          ReadShareGroupStateResponse.toErrorResponseData(topicId, partition, error, "Error in find coordinator. " +
              (exception == null ? error.message() : exception.getMessage()))));
    }

    // Visible for testing
    public CompletableFuture<ReadShareGroupStateResponse> getResult() {
      return result;
    }
  }

  private class SendThread extends InterBrokerSendThread {
    private final ConcurrentLinkedQueue<PersisterStateManagerHandler> queue = new ConcurrentLinkedQueue<>();
    private final Random random;

    public SendThread(String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible, Random random) {
      super(name, networkClient, requestTimeoutMs, time, isInterruptible);
      this.random = random;
    }

    private Node randomNode() {
      List<Node> nodes = cacheHelper.getClusterNodes();
      if (nodes == null || nodes.isEmpty()) {
        return Node.noNode();
      }
      return nodes.get(random.nextInt(nodes.size()));
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
          Node randomNode = randomNode();
          if (randomNode == Node.noNode()) {
            log.error("Unable to find node to use for coordinator lookup.");
            return Collections.emptyList();
          }
          log.info("Sending find coordinator RPC");
          return Collections.singletonList(new RequestAndCompletionHandler(
              time.milliseconds(),
              randomNode,
              handler.findShareCoordinatorBuilder(),
              handler
          ));
        } else {
          log.info("Sending share state RPC - {}", handler.name());
          // share coord node already available
          return Collections.singletonList(new RequestAndCompletionHandler(
              time.milliseconds(),
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

    @Override
    public void doWork() {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
        this.pollOnce(100L);
      } catch (Exception e) {
        log.error("Timed out", e);
      }
    }
  }
}