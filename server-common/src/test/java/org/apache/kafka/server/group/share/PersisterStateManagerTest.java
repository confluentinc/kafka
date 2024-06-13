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
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateRequest;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateRequest;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PersisterStateManagerTest {

  private static final KafkaClient CLIENT = mock(KafkaClient.class);
  private static final Time MOCK_TIME = new MockTime();
  private static final ShareCoordinatorMetadataCacheHelper CACHE_HELPER = mock(ShareCoordinatorMetadataCacheHelper.class);
  private static final int MAX_FIND_COORD_ATTEMPTS = 5;
  public static final long REQUEST_BACKOFF_MS = 1_000L;
  public static final long REQUEST_BACKOFF_MAX_MS = 30_000L;

  private static final String HOST = "localhost";
  private static final int PORT = 9092;

  private static class PersisterStateManagerBuilder {

    private KafkaClient client = CLIENT;
    private Time time = MOCK_TIME;
    private ShareCoordinatorMetadataCacheHelper cacheHelper = CACHE_HELPER;

    private PersisterStateManagerBuilder withKafkaClient(KafkaClient client) {
      this.client = client;
      return this;
    }

    private PersisterStateManagerBuilder withCacheHelper(ShareCoordinatorMetadataCacheHelper cacheHelper) {
      this.cacheHelper = cacheHelper;
      return this;
    }

    private PersisterStateManagerBuilder withTime(Time time) {
      this.time = time;
      return this;
    }

    public static PersisterStateManagerBuilder builder() {
      return new PersisterStateManagerBuilder();
    }

    public PersisterStateManager build() {
      return new PersisterStateManager(client, time, cacheHelper);
    }
  }
  private abstract class TestStateHandler extends PersisterStateManager.PersisterStateManagerHandler {

    private final CompletableFuture<Errors> result;

    TestStateHandler(
        PersisterStateManager stateManager,
        String groupId,
        Uuid topicId,
        int partition,
        CompletableFuture<Errors> result,
        long backoffMs,
        long backoffMaxMs,
        int maxFindCoordAttempts) {
      stateManager.super(groupId, topicId, partition, backoffMs, backoffMaxMs, maxFindCoordAttempts);
      this.result = result;
    }

    @Override
    protected void handleRequestResponse(ClientResponse response) {
      result.complete(Errors.NONE);
    }

    @Override
    protected boolean isRequestResponse(ClientResponse response) {
      return true;
    }

    @Override
    protected void findCoordinatorErrorResponse(Errors error, Exception exception) {
      result.complete(error);
    }

    @Override
    protected String name() {
      return "TestStateHandler";
    }

    CompletableFuture<Errors> getResult() {
      return this.result;
    }
  }

  private ShareCoordinatorMetadataCacheHelper getDefaultCacheHelper(Node suppliedNode) {
    return new ShareCoordinatorMetadataCacheHelper() {
      @Override
      public boolean containsTopic(String topic) {
        return false;
      }

      @Override
      public Node getShareCoordinator(String key, String internalTopicName) {
        return Node.noNode();
      }

      @Override
      public List<Node> getClusterNodes() {
        return Collections.singletonList(suppliedNode);
      }
    };
  }

  private ShareCoordinatorMetadataCacheHelper getCoordinatorCacheHelper(Node coordinatorNode) {
    return new ShareCoordinatorMetadataCacheHelper() {
      @Override
      public boolean containsTopic(String topic) {
        return true;
      }

      @Override
      public Node getShareCoordinator(String key, String internalTopicName) {
        return coordinatorNode;
      }

      @Override
      public List<Node> getClusterNodes() {
        return Collections.emptyList();
      }
    };
  }

  @Test
  public void testFindCoordinatorFatalError() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node suppliedNode = new Node(0, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey(coordinatorKey)
                        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                        .setHost(Node.noNode().host())
                        .setNodeId(Node.noNode().id())
                        .setPort(Node.noNode().port())
                ))
        ),
        suppliedNode
    );

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<Errors> future = new CompletableFuture<>();

    TestStateHandler handler = spy(new TestStateHandler(
        stateManager,
        groupId,
        topicId,
        partition,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ) {
      @Override
      protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
        return null;
      }
    });

    stateManager.enqueue(handler);

    CompletableFuture<Errors> resultFuture = handler.getResult();

    Errors result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code(), result.code());
    verify(handler, times(1)).findShareCoordinatorBuilder();

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testFindCoordinatorAttemptsExhausted() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node suppliedNode = new Node(0, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey(coordinatorKey)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setHost(Node.noNode().host())
                        .setNodeId(Node.noNode().id())
                        .setPort(Node.noNode().port())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey(coordinatorKey)
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                        .setHost(Node.noNode().host())
                        .setNodeId(Node.noNode().id())
                        .setPort(Node.noNode().port())
                ))
        ),
        suppliedNode
    );

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<Errors> future = new CompletableFuture<>();

    int maxAttempts = 2;

    TestStateHandler handler = spy(new TestStateHandler(
        stateManager,
        groupId,
        topicId,
        partition,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        maxAttempts
    ) {
      @Override
      protected AbstractRequest.Builder<? extends AbstractRequest> requestBuilder() {
        return null;
      }
    });

    stateManager.enqueue(handler);

    CompletableFuture<Errors> resultFuture = handler.getResult();

    Errors result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code(), result.code());
    verify(handler, times(2)).findShareCoordinatorBuilder();

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testFindCoordinatorSuccess() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node suppliedNode = new Node(0, HOST, PORT);
    Node coordinatorNode = new Node(1, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setNodeId(1)
                        .setHost(HOST)
                        .setPort(PORT)
                        .setErrorCode(Errors.NONE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> {
      ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new ReadShareGroupStateResponse(
        new ReadShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new ReadShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                            .setStateEpoch(1)
                            .setStartOffset(0)
                            .setStateBatches(Collections.emptyList())
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
        groupId,
        topicId,
        partition,
        0,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.getResult();

    ReadShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    verify(handler, times(1)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testWriteStateRequestCoordinatorFoundSuccessfully() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;
    List<PersisterStateBatch> stateBatches = Arrays.asList(
        new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
        new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
    );

    Node suppliedNode = new Node(0, HOST, PORT);
    Node coordinatorNode = new Node(1, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setNodeId(1)
                        .setHost(HOST)
                        .setPort(PORT)
                        .setErrorCode(Errors.NONE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> {
      WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new WriteShareGroupStateResponse(
        new WriteShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new WriteShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
        groupId,
        topicId,
        partition,
        0,
        0,
        0,
        stateBatches,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.getResult();

    WriteShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(1)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testWriteStateRequestCoordinatorFoundOnRetry() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;
    List<PersisterStateBatch> stateBatches = Arrays.asList(
        new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
        new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
    );

    Node suppliedNode = new Node(0, HOST, PORT);
    Node coordinatorNode = new Node(1, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setNodeId(1)
                        .setHost(HOST)
                        .setPort(PORT)
                        .setErrorCode(Errors.NONE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> {
      WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new WriteShareGroupStateResponse(
        new WriteShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new WriteShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
        groupId,
        topicId,
        partition,
        0,
        0,
        0,
        stateBatches,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.getResult();

    WriteShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(2)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testWriteStateRequestWithCoordinatorNode() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;
    List<PersisterStateBatch> stateBatches = Arrays.asList(
        new PersisterStateBatch(0, 9, (byte) 0, (short) 1),
        new PersisterStateBatch(10, 19, (byte) 1, (short) 1)
    );

    Node coordinatorNode = new Node(1, HOST, PORT);

    client.prepareResponseFrom(body -> {
      WriteShareGroupStateRequest request = (WriteShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new WriteShareGroupStateResponse(
        new WriteShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new WriteShareGroupStateResponseData.WriteStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new WriteShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<WriteShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.WriteStateHandler handler = spy(stateManager.new WriteStateHandler(
        groupId,
        topicId,
        partition,
        0,
        0,
        0,
        stateBatches,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<WriteShareGroupStateResponse> resultFuture = handler.getResult();

    WriteShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    WriteShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(0)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testReadStateRequestCoordinatorFoundSuccessfully() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node suppliedNode = new Node(0, HOST, PORT);
    Node coordinatorNode = new Node(1, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setNodeId(1)
                        .setHost(HOST)
                        .setPort(PORT)
                        .setErrorCode(Errors.NONE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> {
      ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new ReadShareGroupStateResponse(
        new ReadShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new ReadShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                            .setStateEpoch(1)
                            .setStartOffset(0)
                            .setStateBatches(Collections.emptyList())
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
        groupId,
        topicId,
        partition,
        0,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.getResult();

    ReadShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(1)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());
    assertEquals(partitionResult.stateEpoch(), 1);
    assertEquals(partitionResult.startOffset(), 0);

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testReadStateRequestCoordinatorFoundOnRetry() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node suppliedNode = new Node(0, HOST, PORT);
    Node coordinatorNode = new Node(1, HOST, PORT);

    String coordinatorKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partition);

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> body instanceof FindCoordinatorRequest
            && ((FindCoordinatorRequest) body).data().keyType() == FindCoordinatorRequest.CoordinatorType.SHARE.id()
            && ((FindCoordinatorRequest) body).data().coordinatorKeys().get(0).equals(coordinatorKey),
        new FindCoordinatorResponse(
            new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setNodeId(1)
                        .setHost(HOST)
                        .setPort(PORT)
                        .setErrorCode(Errors.NONE.code())
                ))
        ),
        suppliedNode
    );

    client.prepareResponseFrom(body -> {
      ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new ReadShareGroupStateResponse(
        new ReadShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new ReadShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                            .setStateEpoch(1)
                            .setStartOffset(0)
                            .setStateBatches(Collections.emptyList())
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getDefaultCacheHelper(suppliedNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
        groupId,
        topicId,
        partition,
        0,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.getResult();

    ReadShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(2)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the FIND_COORDINATOR request
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());
    assertEquals(partitionResult.stateEpoch(), 1);
    assertEquals(partitionResult.startOffset(), 0);

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }

  @Test
  public void testReadStateRequestWithCoordinatorNode() {

    MockClient client = new MockClient(MOCK_TIME);

    String groupId = "group1";
    Uuid topicId = Uuid.randomUuid();
    int partition = 10;

    Node coordinatorNode = new Node(1, HOST, PORT);

    client.prepareResponseFrom(body -> {
      ReadShareGroupStateRequest request = (ReadShareGroupStateRequest) body;
      String requestGroupId = request.data().groupId();
      Uuid requestTopicId = request.data().topics().get(0).topicId();
      int requestPartition = request.data().topics().get(0).partitions().get(0).partition();

      return requestGroupId.equals(groupId) && requestTopicId == topicId && requestPartition == partition;
    }, new ReadShareGroupStateResponse(
        new ReadShareGroupStateResponseData()
            .setResults(Collections.singletonList(
                new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(topicId)
                    .setPartitions(Collections.singletonList(
                        new ReadShareGroupStateResponseData.PartitionResult()
                            .setPartition(partition)
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage("")
                            .setStateEpoch(1)
                            .setStartOffset(0)
                            .setStateBatches(Collections.emptyList())
                    ))
            ))
    ), coordinatorNode);

    ShareCoordinatorMetadataCacheHelper cacheHelper = getCoordinatorCacheHelper(coordinatorNode);

    PersisterStateManager stateManager = PersisterStateManagerBuilder.builder()
        .withKafkaClient(client)
        .withCacheHelper(cacheHelper)
        .build();

    stateManager.start();

    CompletableFuture<ReadShareGroupStateResponse> future = new CompletableFuture<>();

    PersisterStateManager.ReadStateHandler handler = spy(stateManager.new ReadStateHandler(
        groupId,
        topicId,
        partition,
        0,
        future,
        REQUEST_BACKOFF_MS,
        REQUEST_BACKOFF_MAX_MS,
        MAX_FIND_COORD_ATTEMPTS
    ));

    stateManager.enqueue(handler);

    CompletableFuture<ReadShareGroupStateResponse> resultFuture = handler.getResult();

    ReadShareGroupStateResponse result = null;
    try {
      result = resultFuture.get();
    } catch (Exception e) {
      fail("Failed to get result from future", e);
    }

    ReadShareGroupStateResponseData.PartitionResult partitionResult = result.data().results().get(0).partitions().get(0);

    verify(handler, times(0)).findShareCoordinatorBuilder();
    verify(handler, times(1)).requestBuilder();

    // Verifying the coordinator node was populated correctly by the constructor
    assertEquals(handler.getCoordinatorNode(), coordinatorNode);

    // Verifying the result returned in correct
    assertEquals(partitionResult.partition(), partition);
    assertEquals(partitionResult.errorCode(), Errors.NONE.code());
    assertEquals(partitionResult.stateEpoch(), 1);
    assertEquals(partitionResult.startOffset(), 0);

    try {
      // Stopping the state manager
      stateManager.stop();
    } catch (InterruptedException e) {
      fail("Failed to stop state manager", e);
    }
  }
}