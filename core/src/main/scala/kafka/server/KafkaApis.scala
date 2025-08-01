/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.{QuotaManagers, UNBOUNDED_QUOTA}
import kafka.server.handlers.DescribeTopicPartitionsRequestHandler
import kafka.server.share.{ShareFetchUtils, SharePartitionManager}
import kafka.utils.Logging
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.EndpointType
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclOperation._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic.{GROUP_METADATA_TOPIC_NAME, SHARE_GROUP_STATE_TOPIC_NAME, TRANSACTION_STATE_TOPIC_NAME, isInternal}
import org.apache.kafka.common.internals.{FatalExitError, Plugin, Topic}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.{AddPartitionsToTxnResult, AddPartitionsToTxnResultCollection}
import org.apache.kafka.common.message.DeleteRecordsResponseData.{DeleteRecordsPartitionResult, DeleteRecordsTopicResult}
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData.DeleteShareGroupOffsetsRequestTopic
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.{MetadataResponsePartition, MetadataResponseTopic}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult, OffsetForLeaderTopicResultCollection}
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType._
import org.apache.kafka.common.resource.{Resource, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.security.token.delegation.{DelegationToken, TokenInformation}
import org.apache.kafka.common.utils.{ProducerIdAndEpoch, Time}
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.{Group, GroupConfig, GroupConfigManager, GroupCoordinator}
import org.apache.kafka.coordinator.share.ShareCoordinator
import org.apache.kafka.metadata.{ConfigRepository, MetadataCache}
import org.apache.kafka.server.{ApiVersionManager, ClientMetricsManager, DelegationTokenManager, ProcessRole}
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.common.{GroupVersion, RequestLocal, ShareVersion, StreamsVersion, TransactionVersion}
import org.apache.kafka.server.config.DelegationTokenManagerConfigs
import org.apache.kafka.server.share.context.ShareFetchContext
import org.apache.kafka.server.share.{ErroneousAndValidPartitionData, SharePartitionKey}
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.storage.internals.log.AppendOrigin
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.stream.Collectors
import java.util.function.Supplier
import java.util.{Collections, Optional}
import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, Set, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.javaapi.OptionConverters

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val forwardingManager: ForwardingManager,
                val replicaManager: ReplicaManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val shareCoordinator: ShareCoordinator,
                val autoTopicCreationManager: AutoTopicCreationManager,
                val brokerId: Int,
                val config: KafkaConfig,
                val configRepository: ConfigRepository,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizerPlugin: Option[Plugin[Authorizer]],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                val sharePartitionManager: SharePartitionManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val apiVersionManager: ApiVersionManager,
                val clientMetricsManager: ClientMetricsManager,
                val groupConfigManager: GroupConfigManager,
                val brokerEpochSupplier: Supplier[java.lang.Long]
) extends ApiRequestHandler with Logging {

  type ProduceResponseStats = Map[TopicIdPartition, RecordValidationStats]
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)
  val configHelper = new ConfigHelper(metadataCache, config, configRepository)
  val authHelper = new AuthHelper(authorizerPlugin)
  val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
  val aclApis = new AclApis(authHelper, authorizerPlugin, requestHelper, ProcessRole.BrokerRole, config)
  val configManager = new ConfigAdminManager(brokerId, config, configRepository)
  val describeTopicPartitionsRequestHandler = new DescribeTopicPartitionsRequestHandler(
    metadataCache, authHelper, config)

  def close(): Unit = {
    aclApis.close()
    info("Shutdown complete.")
  }

  private def forwardToController(request: RequestChannel.Request): Unit = {
    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) => requestHelper.sendForwardedResponse(request, response)
        case None => handleInvalidVersionsDuringForwarding(request)
      }
    }

    forwardingManager.forwardRequest(request, responseCallback)
  }

  private def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = {
    info(s"The client connection will be closed due to controller responded " +
      s"unsupported version exception during $request forwarding. " +
      s"This could happen when the controller changed after the connection was established.")
    requestChannel.closeConnection(request, Collections.emptyMap())
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    def handleError(e: Throwable): Unit = {
      error(s"Unexpected error handling request ${request.requestDesc(true)} " +
        s"with context ${request.context}", e)
      requestHelper.handleError(request, e)
    }

    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      if (!apiVersionManager.isApiEnabled(request.header.apiKey, request.header.apiVersion)) {
        // The socket server will reject APIs which are not exposed in this scope and close the connection
        // before handing them to the request handler, so this path should not be exercised in practice
        throw new IllegalStateException(s"API ${request.header.apiKey} with version ${request.header.apiVersion} is not enabled")
      }

      request.header.apiKey match {
        case ApiKeys.PRODUCE => handleProduceRequest(request, requestLocal)
        case ApiKeys.FETCH => handleFetchRequest(request)
        case ApiKeys.LIST_OFFSETS => handleListOffsetRequest(request)
        case ApiKeys.METADATA => handleTopicMetadataRequest(request)
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.OFFSET_FETCH => handleOffsetFetchRequest(request).exceptionally(handleError)
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.HEARTBEAT => handleHeartbeatRequest(request).exceptionally(handleError)
        case ApiKeys.LEAVE_GROUP => handleLeaveGroupRequest(request).exceptionally(handleError)
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_GROUPS => handleDescribeGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.LIST_GROUPS => handleListGroupsRequest(request).exceptionally(handleError)
        case ApiKeys.SASL_HANDSHAKE => handleSaslHandshakeRequest(request)
        case ApiKeys.API_VERSIONS => handleApiVersionsRequest(request)
        case ApiKeys.CREATE_TOPICS => forwardToController(request)
        case ApiKeys.DELETE_TOPICS => forwardToController(request)
        case ApiKeys.DELETE_RECORDS => handleDeleteRecordsRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request, requestLocal)
        case ApiKeys.OFFSET_FOR_LEADER_EPOCH => handleOffsetForLeaderEpochRequest(request)
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionsToTxnRequest(request, requestLocal)
        case ApiKeys.ADD_OFFSETS_TO_TXN => handleAddOffsetsToTxnRequest(request, requestLocal)
        case ApiKeys.END_TXN => handleEndTxnRequest(request, requestLocal)
        case ApiKeys.WRITE_TXN_MARKERS => handleWriteTxnMarkersRequest(request, requestLocal)
        case ApiKeys.TXN_OFFSET_COMMIT => handleTxnOffsetCommitRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_ACLS => handleDescribeAcls(request)
        case ApiKeys.CREATE_ACLS => forwardToController(request)
        case ApiKeys.DELETE_ACLS => forwardToController(request)
        case ApiKeys.ALTER_CONFIGS => handleAlterConfigsRequest(request)
        case ApiKeys.DESCRIBE_CONFIGS => handleDescribeConfigsRequest(request)
        case ApiKeys.ALTER_REPLICA_LOG_DIRS => handleAlterReplicaLogDirsRequest(request)
        case ApiKeys.DESCRIBE_LOG_DIRS => handleDescribeLogDirsRequest(request)
        case ApiKeys.SASL_AUTHENTICATE => handleSaslAuthenticateRequest(request)
        case ApiKeys.CREATE_PARTITIONS => forwardToController(request)
        // Create, renew and expire DelegationTokens must first validate that the connection
        // itself is not authenticated with a delegation token before maybeForwardToController.
        case ApiKeys.CREATE_DELEGATION_TOKEN => handleCreateTokenRequest(request)
        case ApiKeys.RENEW_DELEGATION_TOKEN => handleRenewTokenRequest(request)
        case ApiKeys.EXPIRE_DELEGATION_TOKEN => handleExpireTokenRequest(request)
        case ApiKeys.DESCRIBE_DELEGATION_TOKEN => handleDescribeTokensRequest(request)
        case ApiKeys.DELETE_GROUPS => handleDeleteGroupsRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.ELECT_LEADERS => forwardToController(request)
        case ApiKeys.INCREMENTAL_ALTER_CONFIGS => handleIncrementalAlterConfigsRequest(request)
        case ApiKeys.ALTER_PARTITION_REASSIGNMENTS => forwardToController(request)
        case ApiKeys.LIST_PARTITION_REASSIGNMENTS => forwardToController(request)
        case ApiKeys.OFFSET_DELETE => handleOffsetDeleteRequest(request, requestLocal).exceptionally(handleError)
        case ApiKeys.DESCRIBE_CLIENT_QUOTAS => handleDescribeClientQuotasRequest(request)
        case ApiKeys.ALTER_CLIENT_QUOTAS => forwardToController(request)
        case ApiKeys.DESCRIBE_USER_SCRAM_CREDENTIALS => handleDescribeUserScramCredentialsRequest(request)
        case ApiKeys.ALTER_USER_SCRAM_CREDENTIALS => forwardToController(request)
        case ApiKeys.UPDATE_FEATURES => forwardToController(request)
        case ApiKeys.DESCRIBE_CLUSTER => handleDescribeCluster(request)
        case ApiKeys.DESCRIBE_PRODUCERS => handleDescribeProducersRequest(request)
        case ApiKeys.UNREGISTER_BROKER => forwardToController(request)
        case ApiKeys.DESCRIBE_TRANSACTIONS => handleDescribeTransactionsRequest(request)
        case ApiKeys.LIST_TRANSACTIONS => handleListTransactionsRequest(request)
        case ApiKeys.DESCRIBE_QUORUM => forwardToController(request)
        case ApiKeys.CONSUMER_GROUP_HEARTBEAT => handleConsumerGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.CONSUMER_GROUP_DESCRIBE => handleConsumerGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.DESCRIBE_TOPIC_PARTITIONS => handleDescribeTopicPartitionsRequest(request)
        case ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS => handleGetTelemetrySubscriptionsRequest(request)
        case ApiKeys.PUSH_TELEMETRY => handlePushTelemetryRequest(request)
        case ApiKeys.LIST_CONFIG_RESOURCES => handleListConfigResources(request)
        case ApiKeys.ADD_RAFT_VOTER => forwardToController(request)
        case ApiKeys.REMOVE_RAFT_VOTER => forwardToController(request)
        case ApiKeys.SHARE_GROUP_HEARTBEAT => handleShareGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.SHARE_GROUP_DESCRIBE => handleShareGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.SHARE_FETCH => handleShareFetchRequest(request).exceptionally(handleError)
        case ApiKeys.SHARE_ACKNOWLEDGE => handleShareAcknowledgeRequest(request).exceptionally(handleError)
        case ApiKeys.INITIALIZE_SHARE_GROUP_STATE => handleInitializeShareGroupStateRequest(request).exceptionally(handleError)
        case ApiKeys.READ_SHARE_GROUP_STATE => handleReadShareGroupStateRequest(request).exceptionally(handleError)
        case ApiKeys.WRITE_SHARE_GROUP_STATE => handleWriteShareGroupStateRequest(request).exceptionally(handleError)
        case ApiKeys.DELETE_SHARE_GROUP_STATE => handleDeleteShareGroupStateRequest(request).exceptionally(handleError)
        case ApiKeys.READ_SHARE_GROUP_STATE_SUMMARY => handleReadShareGroupStateSummaryRequest(request).exceptionally(handleError)
        case ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS => handleDescribeShareGroupOffsetsRequest(request).exceptionally(handleError)
        case ApiKeys.ALTER_SHARE_GROUP_OFFSETS => handleAlterShareGroupOffsetsRequest(request).exceptionally(handleError)
        case ApiKeys.DELETE_SHARE_GROUP_OFFSETS => handleDeleteShareGroupOffsetsRequest(request).exceptionally(handleError)
        case ApiKeys.STREAMS_GROUP_DESCRIBE => handleStreamsGroupDescribe(request).exceptionally(handleError)
        case ApiKeys.STREAMS_GROUP_HEARTBEAT => handleStreamsGroupHeartbeat(request).exceptionally(handleError)
        case ApiKeys.GET_REPLICA_LOG_INFO => handleGetReplicaLogInfo(request)
        case _ => throw new IllegalStateException(s"No handler for request api key ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(e)
    } finally {
      // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
      // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
      // expiration thread for certain delayed operations (e.g. DelayedJoin)
      // Delayed fetches are also completed by ReplicaFetcherThread.
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  def handleGetReplicaLogInfo(request: RequestChannel.Request): Unit = {
    var partitionCount = 0
    def processPartitions(topicLogInfo: GetReplicaLogInfoResponseData.TopicPartitionLogInfo,
                          partitionIter: util.Iterator[Integer],
                          action: Integer => GetReplicaLogInfoResponseData.PartitionLogInfo): Unit = {
      while (partitionIter.hasNext && partitionCount < GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST) {
        topicLogInfo.partitionLogInfo().add(action(partitionIter.next()))
        partitionCount += 1
      }
    }

    val isAuthorizedClusterAction = authorizeClusterOperation(request, CLUSTER_ACTION)
    def isAuthorized(topicName: String): Boolean =
      isAuthorizedClusterAction || authHelper.authorize(request.context, DESCRIBE, TOPIC, topicName)

    val getReplicaLogInfoRequest = request.body[GetReplicaLogInfoRequest]
    val data = getReplicaLogInfoRequest.data()

    val topicIter = data.topicPartitions().iterator()
    var previousPartitionIter: Option[util.Iterator[Integer]] = None
    val responseData = new GetReplicaLogInfoResponseData()
      .setBrokerEpoch(brokerEpochSupplier.get())

    while (topicIter.hasNext && partitionCount < GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST) {
      val topic = topicIter.next()
      val partitionIter = topic.partitions().iterator()
      previousPartitionIter = Some(partitionIter)

      val topicPartitionLogInfo = new GetReplicaLogInfoResponseData.TopicPartitionLogInfo()
        .setTopicId(topic.topicId())

      val maybeTopicName = metadataCache.getTopicName(topic.topicId())
      if (maybeTopicName.isEmpty) {
        processPartitions(topicPartitionLogInfo, partitionIter,
          new GetReplicaLogInfoResponseData.PartitionLogInfo()
            .setPartition(_)
            .setErrorCode(Errors.UNKNOWN_TOPIC_ID.code()))
      } else if (!isAuthorized(maybeTopicName.get())) {
        processPartitions(topicPartitionLogInfo, partitionIter,
          new GetReplicaLogInfoResponseData.PartitionLogInfo()
            .setPartition(_)
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code()))
      } else {
        val topicName = maybeTopicName.get()
        processPartitions(topicPartitionLogInfo, partitionIter, { partitionId: Integer =>
          val topicPartition = new TopicPartition(topicName, partitionId)
          replicaManager.getPartitionOrError(topicPartition) match {
            case Left(err) => new GetReplicaLogInfoResponseData.PartitionLogInfo()
              .setPartition(topicPartition.partition())
              .setErrorCode(err.code())
            case Right(partition) => partition.log match {
              case None => new GetReplicaLogInfoResponseData.PartitionLogInfo()
                .setErrorCode(Errors.LOG_DIR_NOT_FOUND.code())
              case Some(log) => {
                val logEndOffset = log.logEndOffset
                val lastLeaderEpoch = log.latestEpoch.orElse(-1)
                val leaderEpoch = partition.getLeaderEpoch
                new GetReplicaLogInfoResponseData.PartitionLogInfo()
                  .setPartition(partitionId)
                  .setLogEndOffset(logEndOffset)
                  .setCurrentLeaderEpoch(leaderEpoch)
                  .setLastWrittenLeaderEpoch(lastLeaderEpoch)
              }
            }
          }
        })
      }
      responseData.topicPartitionLogInfoList().add(topicPartitionLogInfo)
    }
    responseData.setHasMoreData(topicIter.hasNext || previousPartitionIter.map(_.hasNext).getOrElse(false))
    requestHelper.sendMaybeThrottle(request, new GetReplicaLogInfoResponse(responseData))
  }

  override def tryCompleteActions(): Unit = {
    replicaManager.tryCompleteActions()
  }

  /**
   * Handle an offset commit request
   */
  def handleOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    // Reject the request if not authorized to the group.
    if (!authHelper.authorize(request.context, READ, GROUP, offsetCommitRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val useTopicIds = OffsetCommitResponse.useTopicIds(request.header.apiVersion)

      if (useTopicIds) {
        offsetCommitRequest.data.topics.forEach { topic =>
          if (topic.topicId != Uuid.ZERO_UUID) {
            metadataCache.getTopicName(topic.topicId).ifPresent(name => topic.setName(name))
          }
        }
      }

      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = OffsetCommitResponse.newBuilder(useTopicIds)
      val authorizedTopicsRequest = new mutable.ArrayBuffer[OffsetCommitRequestData.OffsetCommitRequestTopic]()
      offsetCommitRequest.data.topics.forEach { topic =>
        if (useTopicIds && topic.name.isEmpty) {
          // If the topic name is undefined, it means that the topic id is unknown so we add
          // the topic and all its partitions to the response with UNKNOWN_TOPIC_ID.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.topicId, topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_ID)
        } else if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.topicId, topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetCommitRequestData.OffsetCommitRequestPartition](
            topic.topicId, topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetCommitRequestData.OffsetCommitRequestTopic()
            .setTopicId(topic.topicId)
            .setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).isPresent) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(
                topic.topicId,
                topic.name,
                partition.partitionIndex,
                Errors.UNKNOWN_TOPIC_OR_PARTITION
              )
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicsRequest += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicsRequest.isEmpty) {
        requestHelper.sendMaybeThrottle(request, responseBuilder.build())
        CompletableFuture.completedFuture(())
      } else {
        groupCoordinator.commitOffsets(
          request.context,
          new OffsetCommitRequestData()
            .setGroupId(offsetCommitRequest.data.groupId)
            .setMemberId(offsetCommitRequest.data.memberId)
            .setGenerationIdOrMemberEpoch(offsetCommitRequest.data.generationIdOrMemberEpoch)
            .setRetentionTimeMs(offsetCommitRequest.data.retentionTimeMs)
            .setGroupInstanceId(offsetCommitRequest.data.groupInstanceId)
            .setTopics(authorizedTopicsRequest.asJava),
          requestLocal.bufferSupplier
        ).handle[Unit] { (results, exception) =>
          if (exception != null) {
            requestHelper.sendMaybeThrottle(request, offsetCommitRequest.getErrorResponse(exception))
          } else {
            requestHelper.sendMaybeThrottle(request, responseBuilder.merge(results).build())
          }
        }
      }
    }
  }

  case class LeaderNode(leaderId: Int, leaderEpoch: Int, node: Option[Node])

  private def getCurrentLeader(tp: TopicPartition, ln: ListenerName): LeaderNode = {
    val partitionInfoOrError = replicaManager.getPartitionOrError(tp)
    val (leaderId, leaderEpoch) = partitionInfoOrError match {
      case Right(x) =>
        (x.leaderReplicaIdOpt.getOrElse(-1), x.getLeaderEpoch)
      case Left(x) =>
        debug(s"Unable to retrieve local leaderId and Epoch with error $x, falling back to metadata cache")
        OptionConverters.toScala(metadataCache.getLeaderAndIsr(tp.topic, tp.partition)) match {
          case Some(pinfo) => (pinfo.leader(), pinfo.leaderEpoch())
          case None => (-1, -1)
        }
    }
    LeaderNode(leaderId, leaderEpoch, OptionConverters.toScala(metadataCache.getAliveBrokerNode(leaderId, ln)))
  }

  /**
   * Handle a produce request
   */
  def handleProduceRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val produceRequest = request.body[ProduceRequest]

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    val unauthorizedTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicIdPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicIdPartition, MemoryRecords]()
    val topicIdToPartitionData = new mutable.ArrayBuffer[(TopicIdPartition, ProduceRequestData.PartitionProduceData)]

    produceRequest.data.topicData.forEach { topic =>
      topic.partitionData.forEach { partition =>
        val (topicName, topicId) = if (topic.topicId().equals(Uuid.ZERO_UUID)) {
          (topic.name(), metadataCache.getTopicId(topic.name()))
        } else {
          (metadataCache.getTopicName(topic.topicId).orElse(topic.name), topic.topicId())
        }

        val topicPartition = new TopicPartition(topicName, partition.index())
        // To be compatible with the old version, only return UNKNOWN_TOPIC_ID if request version uses topicId, but the corresponding topic name can't be found.
        if (topicName.isEmpty && request.header.apiVersion > 12)
          nonExistingTopicResponses += new TopicIdPartition(topicId, topicPartition) -> new PartitionResponse(Errors.UNKNOWN_TOPIC_ID)
        else
          topicIdToPartitionData += new TopicIdPartition(topicId, topicPartition) -> partition
      }
    }
    // cache the result to avoid redundant authorization calls
    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC, topicIdToPartitionData)(_._1.topic)

    topicIdToPartitionData.foreach { case (topicIdPartition, partition) =>
      // This caller assumes the type is MemoryRecords and that is true on current serialization
      // We cast the type to avoid causing big change to code base.
      // https://issues.apache.org/jira/browse/KAFKA-10698
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicIdPartition.topic))
        unauthorizedTopicResponses += topicIdPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicIdPartition.topicPartition))
        nonExistingTopicResponses += topicIdPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          authorizedRequestInfo += (topicIdPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicIdPartition -> new PartitionResponse(Errors.forException(e))
        }
    }

    // the callback for sending a produce response
    // The construction of ProduceResponse is able to accept auto-generated protocol data so
    // KafkaApis#handleProduceRequest should apply auto-generated protocol to avoid extra conversion.
    // https://issues.apache.org/jira/browse/KAFKA-10730
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicIdPartition, PartitionResponse]): Unit = {
      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      val nodeEndpoints = new mutable.HashMap[Int, Node]
      mergedResponseStatus.foreachEntry { (topicIdPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicIdPartition,
            status.error.exceptionName))

          if (request.header.apiVersion >= 10) {
            status.error match {
              case Errors.NOT_LEADER_OR_FOLLOWER =>
                val leaderNode = getCurrentLeader(topicIdPartition.topicPartition(), request.context.listenerName)
                leaderNode.node.foreach { node =>
                  nodeEndpoints.put(node.id(), node)
                }
                status.currentLeader
                  .setLeaderId(leaderNode.leaderId)
                  .setLeaderEpoch(leaderNode.leaderEpoch)
                case _ =>
            }
          }
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()
      val requestSize = request.sizeInBytes
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request.session, request.header.clientId(), requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val maxThrottleTimeMs = Math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
          requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
        } else {
          requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
        }
      }

      // Send the response immediately. In case of throttling, the channel has already been muted.
      if (produceRequest.acks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs, nodeEndpoints.values.toList.asJava), None)
      }
    }

    def processingStatsCallback(processingStats: ProduceResponseStats): Unit = {
      processingStats.foreachEntry { (topicIdPartition, info) =>
        updateRecordConversionStats(request, topicIdPartition.topicPartition(), info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == "__admin_client"
      val transactionSupportedOperation = AddPartitionsToTxnManager.produceRequestVersionToTransactionSupportedOperation(request.header.apiVersion())
      // call the replica manager to append messages to the replicas
      replicaManager.handleProduceAppend(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        transactionalId = produceRequest.transactionalId,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        recordValidationStatsCallback = processingStatsCallback,
        requestLocal = requestLocal,
        transactionSupportedOperation = transactionSupportedOperation)

      // if the request is put into the purgatory, it will have a held reference and hence cannot be garbage collected;
      // hence we clear its data here in order to let GC reclaim its memory since it is already appended to log
      produceRequest.clearPartitionRecords()
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val topicNames =
      if (fetchRequest.version() >= 13)
        metadataCache.topicIdsToNames()
      else
        Collections.emptyMap[Uuid, String]()

    val fetchData = fetchRequest.fetchData(topicNames)
    val forgottenTopics = fetchRequest.forgottenTopics(topicNames)

    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames)

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicIdPartition, data) =>
          if (topicIdPartition.topic == null)
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
          else if (!metadataCache.contains(topicIdPartition.topicPartition))
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += topicIdPartition -> data
        }
      } else {
        fetchContext.foreachPartition { (topicIdPartition, _) =>
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val partitionDatas = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicIdPartition, partitionData) =>
        if (topicIdPartition.topic == null)
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        else
          partitionDatas += topicIdPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topicPartition.topic)
      partitionDatas.foreach { case (topicIdPartition, data) =>
        if (!authorizedTopics.contains(topicIdPartition.topic))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += topicIdPartition -> data
      }
    }

    def maybeDownConvertStorageError(error: Errors): Errors = {
      // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
      // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
      // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
      // by converting the KafkaStorageException to NotLeaderOrFollowerException in the response if FetchRequest version <= 5
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      val nodeEndpoints = new mutable.HashMap[Int, Node]
      responsePartitionData.foreach { case (topicIdPartition, data) =>
        val abortedTransactions = data.abortedTransactions.orElse(null)
        val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(topicIdPartition)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(topicIdPartition.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.orElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))

        if (versionId >= 16) {
          data.error match {
            case Errors.NOT_LEADER_OR_FOLLOWER | Errors.FENCED_LEADER_EPOCH =>
              val leaderNode = getCurrentLeader(topicIdPartition.topicPartition(), request.context.listenerName)
              leaderNode.node.foreach { node =>
                nodeEndpoints.put(node.id(), node)
              }
              partitionData.currentLeader()
                .setLeaderId(leaderNode.leaderId)
                .setLeaderEpoch(leaderNode.leaderEpoch)
            case _ =>
          }
        }

        data.divergingEpoch.ifPresent(epoch => partitionData.setDivergingEpoch(epoch))
        partitions.put(topicIdPartition, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      def recordBytesOutMetric(fetchResponse: FetchResponse): Unit = {
        // record the bytes out metrics only when the response is being sent
        fetchResponse.data.responses.forEach { topicResponse =>
          topicResponse.partitions.forEach { data =>
            // If the topic name was not known, we will have no bytes out.
            if (topicResponse.topic != null) {
              val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, data.partitionIndex))
              brokerTopicStats.updateBytesOut(tp.topic, fetchRequest.isFromFollower, reassigningPartitions.contains(tp), FetchResponse.recordsSize(data))
            }
          }
        }
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val fetchResponse = fetchContext.updateAndGenerateResponseData(partitions, Seq.empty.asJava)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, fetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        val responsePartitionsSize = fetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
        trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
          s"metadata=${fetchResponse.sessionId}")
        recordBytesOutMetric(fetchResponse)
        requestHelper.sendResponseExemptThrottle(request, fetchResponse)
      } else {
        // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
        // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
        // quotas. When throttled, we unrecord the recorded bandwidth quota value.
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()
        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request.session, request.header.clientId(), responseSize, timeMs)

        val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
        val fetchResponse = if (maxThrottleTimeMs > 0) {
          request.apiThrottleTimeMs = maxThrottleTimeMs
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request.session, request.header.clientId(), responseSize, timeMs)
          if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
            requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
          } else {
            requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
          }
          // If throttling is required, return an empty response.
          fetchContext.getThrottledResponse(maxThrottleTimeMs, nodeEndpoints.values.toSeq.asJava)
        } else {
          // Get the actual response. This will update the fetch context.
          val fetchResponse = fetchContext.updateAndGenerateResponseData(partitions, nodeEndpoints.values.toSeq.asJava)
          val responsePartitionsSize = fetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
          trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
            s"metadata=${fetchResponse.sessionId}")
          fetchResponse
        }

        recordBytesOutMetric(fetchResponse)
        // Send the response immediately.
        requestChannel.sendResponse(request, fetchResponse, None)
      }
    }

    if (interesting.isEmpty) {
      processResponseCallback(Seq.empty)
    } else {
      // for fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being throttled given
      // no bytes were recorded in the recent quota window
      // trying to fetch more bytes would result in a guaranteed throttling potentially blocking consumer progress
      val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
        Int.MaxValue
      else
        quotas.fetch.maxValueInQuotaWindow(request.session, clientId).toInt

      val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] = if (versionId >= 11) {
        // Fetch API version 11 added preferred replica logic
        Optional.of(new DefaultClientMetadata(
          fetchRequest.rackId,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))
      } else {
        Optional.empty()
      }

      val params = new FetchParams(
        fetchRequest.replicaId,
        fetchRequest.replicaEpoch,
        fetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.of(fetchRequest),
        clientMetadata
      )

      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        params = params,
        fetchInfos = interesting,
        quota = replicationQuota(fetchRequest),
        responseCallback = processResponseCallback,
      )
    }
  }

  def replicationQuota(fetchRequest: FetchRequest): ReplicaQuota =
    if (fetchRequest.isFromFollower) quotas.leader else UNBOUNDED_QUOTA

  def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    val correlationId = request.header.correlationId
    val clientId = request.header.clientId
    val offsetRequest = request.body[ListOffsetsRequest]
    val version = request.header.apiVersion

    def buildErrorResponse(e: Errors, partition: ListOffsetsPartition): ListOffsetsPartitionResponse = {
      new ListOffsetsPartitionResponse()
        .setPartitionIndex(partition.partitionIndex)
        .setErrorCode(e.code)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
    }

    val (authorizedRequestInfo, unauthorizedRequestInfo) = authHelper.partitionSeqByAuthorized(request.context,
        DESCRIBE, TOPIC, offsetRequest.topics.asScala.toSeq)(_.name)

    val unauthorizedResponseStatus = unauthorizedRequestInfo.map(topic =>
      new ListOffsetsTopicResponse()
        .setName(topic.name)
        .setPartitions(topic.partitions.asScala.map(partition =>
          buildErrorResponse(Errors.TOPIC_AUTHORIZATION_FAILED, partition)).asJava)
    ).asJava

    def sendResponseCallback(response: util.Collection[ListOffsetsTopicResponse]): Void = {
      val mergedResponses = new util.ArrayList(response)
      mergedResponses.addAll(unauthorizedResponseStatus)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ListOffsetsResponse(new ListOffsetsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(mergedResponses)))
      null
    }

    if (authorizedRequestInfo.isEmpty) {
      sendResponseCallback(util.List.of)
    } else {
      replicaManager.fetchOffset(authorizedRequestInfo, offsetRequest.duplicatePartitions().asScala,
        offsetRequest.isolationLevel(), offsetRequest.replicaId(), clientId, correlationId, version,
        buildErrorResponse, sendResponseCallback, offsetRequest.timeoutMs())
    }
  }

  private def metadataResponseTopic(error: Errors,
                                    topic: String,
                                    topicId: Uuid,
                                    isInternal: Boolean,
                                    partitionData: util.List[MetadataResponsePartition]): MetadataResponseTopic = {
    new MetadataResponseTopic()
      .setErrorCode(error.code)
      .setName(topic)
      .setTopicId(topicId)
      .setIsInternal(isInternal)
      .setPartitions(partitionData)
  }

  private def getTopicMetadata(
    request: RequestChannel.Request,
    fetchAllTopics: Boolean,
    allowAutoTopicCreation: Boolean,
    topics: Set[String],
    listenerName: ListenerName,
    errorUnavailableEndpoints: Boolean,
    errorUnavailableListeners: Boolean
  ): Seq[MetadataResponseTopic] = {
    val topicResponses = metadataCache.getTopicMetadata(topics.asJava, listenerName,
      errorUnavailableEndpoints, errorUnavailableListeners)

    if (topics.isEmpty || topicResponses.size == topics.size || fetchAllTopics) {
      topicResponses.asScala
    } else {
      val nonExistingTopics = topics.diff(topicResponses.asScala.map(_.name).toSet)
      val nonExistingTopicResponses = if (allowAutoTopicCreation) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request.session, request.header.clientId())
        autoTopicCreationManager.createTopics(nonExistingTopics, controllerMutationQuota, Some(request.context))
      } else {
        nonExistingTopics.map { topic =>
          val error = try {
            Topic.validate(topic)
            Errors.UNKNOWN_TOPIC_OR_PARTITION
          } catch {
            case _: InvalidTopicException =>
              Errors.INVALID_TOPIC_EXCEPTION
          }

          metadataResponseTopic(
            error,
            topic,
            metadataCache.getTopicId(topic),
            Topic.isInternal(topic),
            util.Collections.emptyList()
          )
        }
      }

      topicResponses.asScala ++ nonExistingTopicResponses
    }
  }

  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.body[MetadataRequest]
    val requestVersion = request.header.apiVersion

    // Topic IDs are not supported for versions 10 and 11. Topic names can not be null in these versions.
    if (!metadataRequest.isAllTopics) {
      metadataRequest.data.topics.forEach{ topic =>
        if (topic.name == null && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic name can not be null for version ${metadataRequest.version}")
        } else if (topic.topicId != Uuid.ZERO_UUID && metadataRequest.version < 12) {
          throw new InvalidRequestException(s"Topic IDs are not supported in requests for version ${metadataRequest.version}")
        }
      }
    }

    // Check if topicId is presented firstly.
    val topicIds = metadataRequest.topicIds.asScala.toSet.filterNot(_ == Uuid.ZERO_UUID)
    val useTopicId = topicIds.nonEmpty

    // Only get topicIds and topicNames when supporting topicId
    val unknownTopicIds = topicIds.filter(metadataCache.getTopicName(_).isEmpty)
    val knownTopicNames = topicIds.flatMap(id => OptionConverters.toScala(metadataCache.getTopicName(id)))

    val unknownTopicIdsTopicMetadata = unknownTopicIds.map(topicId =>
        metadataResponseTopic(Errors.UNKNOWN_TOPIC_ID, null, topicId, isInternal = false, util.Collections.emptyList())).toSeq

    val topics = if (metadataRequest.isAllTopics)
      metadataCache.getAllTopics.asScala
    else if (useTopicId)
      knownTopicNames
    else
      metadataRequest.topics.asScala.toSet

    val authorizedForDescribeTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
      topics, logIfDenied = !metadataRequest.isAllTopics)(identity)
    var (authorizedTopics, unauthorizedForDescribeTopics) = topics.partition(authorizedForDescribeTopics.contains)
    var unauthorizedForCreateTopics = Set[String]()

    if (authorizedTopics.nonEmpty) {
      val nonExistingTopics = authorizedTopics.filterNot(metadataCache.contains)
      if (metadataRequest.allowAutoTopicCreation && config.autoCreateTopicsEnable && nonExistingTopics.nonEmpty) {
        if (!authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
          val authorizedForCreateTopics = authHelper.filterByAuthorized(request.context, CREATE, TOPIC,
            nonExistingTopics)(identity)
          unauthorizedForCreateTopics = nonExistingTopics.diff(authorizedForCreateTopics)
          authorizedTopics = authorizedTopics.diff(unauthorizedForCreateTopics)
        }
      }
    }

    val unauthorizedForCreateTopicMetadata = unauthorizedForCreateTopics.map(topic =>
      // Set topicId to zero since we will never create topic which topicId
      metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal(topic), util.Collections.emptyList()))

    // do not disclose the existence of topics unauthorized for Describe, so we've not even checked if they exist or not
    val unauthorizedForDescribeTopicMetadata =
      // In case of all topics, don't include topics unauthorized for Describe
      if ((requestVersion == 0 && (metadataRequest.topics == null || metadataRequest.topics.isEmpty)) || metadataRequest.isAllTopics)
        Set.empty[MetadataResponseTopic]
      else if (useTopicId) {
        // Topic IDs are not considered sensitive information, so returning TOPIC_AUTHORIZATION_FAILED is OK
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, null, metadataCache.getTopicId(topic), isInternal = false, util.Collections.emptyList()))
      } else {
        // We should not return topicId when on unauthorized error, so we return zero uuid.
        unauthorizedForDescribeTopics.map(topic =>
          metadataResponseTopic(Errors.TOPIC_AUTHORIZATION_FAILED, topic, Uuid.ZERO_UUID, isInternal = false, util.Collections.emptyList()))
      }

    // In version 0, we returned an error when brokers with replicas were unavailable,
    // while in higher versions we simply don't include the broker in the returned broker list
    val errorUnavailableEndpoints = requestVersion == 0
    // In versions 5 and below, we returned LEADER_NOT_AVAILABLE if a matching listener was not found on the leader.
    // From version 6 onwards, we return LISTENER_NOT_FOUND to enable diagnosis of configuration errors.
    val errorUnavailableListeners = requestVersion >= 6

    val allowAutoCreation = config.autoCreateTopicsEnable && metadataRequest.allowAutoTopicCreation && !metadataRequest.isAllTopics
    val topicMetadata = getTopicMetadata(request, metadataRequest.isAllTopics, allowAutoCreation, authorizedTopics,
      request.context.listenerName, errorUnavailableEndpoints, errorUnavailableListeners)

    var clusterAuthorizedOperations = Int.MinValue // Default value in the schema
    if (requestVersion >= 8) {
      // get cluster authorized operations
      if (requestVersion <= 10) {
        if (metadataRequest.data.includeClusterAuthorizedOperations) {
          if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME))
            clusterAuthorizedOperations = authHelper.authorizedOperations(request, Resource.CLUSTER)
          else
            clusterAuthorizedOperations = 0
        }
      }

      // get topic authorized operations
      if (metadataRequest.data.includeTopicAuthorizedOperations) {
        def setTopicAuthorizedOperations(topicMetadata: Seq[MetadataResponseTopic]): Unit = {
          topicMetadata.foreach { topicData =>
            topicData.setTopicAuthorizedOperations(authHelper.authorizedOperations(request, new Resource(ResourceType.TOPIC, topicData.name)))
          }
        }
        setTopicAuthorizedOperations(topicMetadata)
      }
    }

    val completeTopicMetadata =  unknownTopicIdsTopicMetadata ++
      topicMetadata ++ unauthorizedForCreateTopicMetadata ++ unauthorizedForDescribeTopicMetadata

    val brokers = metadataCache.getAliveBrokerNodes(request.context.listenerName)

    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(completeTopicMetadata.mkString(","),
      brokers.asScala.mkString(","), request.header.correlationId, request.header.clientId))
    val controllerId = metadataCache.getRandomAliveBrokerId.orElse(MetadataResponse.NO_CONTROLLER_ID)

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
       MetadataResponse.prepareResponse(
         requestVersion,
         requestThrottleMs,
         brokers,
         clusterId,
         controllerId,
         completeTopicMetadata.asJava,
         clusterAuthorizedOperations
      ))
  }

  def handleDescribeTopicPartitionsRequest(request: RequestChannel.Request): Unit = {
    val response = describeTopicPartitionsRequestHandler.handleDescribeTopicPartitionsRequest(request)
    trace("Sending topic partitions metadata %s for correlation id %d to client %s".format(response.topics().asScala.mkString(","),
      request.header.correlationId, request.header.clientId))

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
      response.setThrottleTimeMs(requestThrottleMs)
      new DescribeTopicPartitionsResponse(response)
    })
  }

  /**
   * Handle an offset fetch request
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val offsetFetchRequest = request.body[OffsetFetchRequest]
    val groups = offsetFetchRequest.groups()
    val requireStable = offsetFetchRequest.requireStable()

    val futures = new mutable.ArrayBuffer[CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]](groups.size)
    groups.forEach { groupOffsetFetch =>
      val isAllPartitions = groupOffsetFetch.topics == null
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupOffsetFetch.groupId)) {
        futures += CompletableFuture.completedFuture(OffsetFetchResponse.groupError(
          groupOffsetFetch,
          Errors.GROUP_AUTHORIZATION_FAILED,
          request.header.apiVersion()
        ))
      } else if (isAllPartitions) {
        futures += fetchAllOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      } else {
        futures += fetchOffsetsForGroup(
          request.context,
          groupOffsetFetch,
          requireStable
        )
      }
    }

    CompletableFuture.allOf(futures.toArray: _*).handle[Unit] { (_, _) =>
      val groupResponses = new ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseGroup](futures.size)
      futures.foreach(future => groupResponses += future.get())
      requestHelper.sendMaybeThrottle(request, new OffsetFetchResponse.Builder(groupResponses.asJava).build(request.context.apiVersion))
    }
  }

  private def fetchAllOffsetsForGroup(
    requestContext: RequestContext,
    groupFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    val useTopicIds = OffsetFetchRequest.useTopicIds(requestContext.apiVersion)

    groupCoordinator.fetchAllOffsets(
      requestContext,
      groupFetchRequest,
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (groupFetchResponse, exception) =>
      if (exception != null) {
        OffsetFetchResponse.groupError(
          groupFetchRequest,
          Errors.forException(exception),
          requestContext.apiVersion()
        )
      } else if (groupFetchResponse.errorCode() != Errors.NONE.code) {
        groupFetchResponse
      } else {
        // Clients are not allowed to see offsets for topics that are not authorized for Describe.
        val authorizedNames = authHelper.filterByAuthorized(
          requestContext,
          DESCRIBE,
          TOPIC,
          groupFetchResponse.topics.asScala
        )(_.name)

        val topics = new mutable.ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseTopics]
        groupFetchResponse.topics.forEach { topic =>
          if (authorizedNames.contains(topic.name)) {
            if (useTopicIds) {
              // If the topic is not provided by the group coordinator, we set it
              // using the metadata cache.
              if (topic.topicId == Uuid.ZERO_UUID) {
                topic.setTopicId(metadataCache.getTopicId(topic.name))
              }
              // If we don't have the topic id at all, we skip the topic because
              // we can not serialize it without it.
              if (topic.topicId != Uuid.ZERO_UUID) {
                topics += topic
              }
            } else {
              topics += topic
            }
          }
        }
        groupFetchResponse.setTopics(topics.asJava)
      }
    }
  }

  private def fetchOffsetsForGroup(
    requestContext: RequestContext,
    groupFetchRequest: OffsetFetchRequestData.OffsetFetchRequestGroup,
    requireStable: Boolean
  ): CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup] = {
    val useTopicIds = OffsetFetchRequest.useTopicIds(requestContext.apiVersion)

    if (useTopicIds) {
      groupFetchRequest.topics.forEach { topic =>
        if (topic.topicId != Uuid.ZERO_UUID) {
          metadataCache.getTopicName(topic.topicId).ifPresent(name => topic.setName(name))
        }
      }
    }

    // Clients are not allowed to see offsets for topics that are not authorized for Describe.
    val authorizedTopicNames = authHelper.filterByAuthorized(
      requestContext,
      DESCRIBE,
      TOPIC,
      groupFetchRequest.topics.asScala
    )(_.name)

    val authorizedTopics = new mutable.ArrayBuffer[OffsetFetchRequestData.OffsetFetchRequestTopics]
    val errorTopics = new mutable.ArrayBuffer[OffsetFetchResponseData.OffsetFetchResponseTopics]

    def buildErrorResponse(
      topic: OffsetFetchRequestData.OffsetFetchRequestTopics,
      error: Errors
    ): OffsetFetchResponseData.OffsetFetchResponseTopics = {
      val topicResponse = new OffsetFetchResponseData.OffsetFetchResponseTopics()
        .setTopicId(topic.topicId)
        .setName(topic.name)
      topic.partitionIndexes.forEach { partitionIndex =>
        topicResponse.partitions.add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
          .setPartitionIndex(partitionIndex)
          .setCommittedOffset(-1)
          .setErrorCode(error.code))
      }
      topicResponse
    }

    groupFetchRequest.topics.forEach { topic =>
      if (useTopicIds && topic.name.isEmpty) {
        errorTopics += buildErrorResponse(topic, Errors.UNKNOWN_TOPIC_ID)
      } else if (!authorizedTopicNames.contains(topic.name)) {
        errorTopics += buildErrorResponse(topic, Errors.TOPIC_AUTHORIZATION_FAILED)
      } else {
        authorizedTopics += topic
      }
    }

    groupCoordinator.fetchOffsets(
      requestContext,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId(groupFetchRequest.groupId)
        .setMemberId(groupFetchRequest.memberId)
        .setMemberEpoch(groupFetchRequest.memberEpoch)
        .setTopics(authorizedTopics.asJava),
      requireStable
    ).handle[OffsetFetchResponseData.OffsetFetchResponseGroup] { (groupFetchResponse, exception) =>
      if (exception != null) {
        OffsetFetchResponse.groupError(
          groupFetchRequest,
          Errors.forException(exception),
          requestContext.apiVersion()
        )
      } else if (groupFetchResponse.errorCode() != Errors.NONE.code) {
        groupFetchResponse
      } else {
        val topics = new util.ArrayList[OffsetFetchResponseData.OffsetFetchResponseTopics](
          groupFetchRequest.topics.size + errorTopics.size
        )
        topics.addAll(groupFetchResponse.topics)
        topics.addAll(errorTopics.asJava)
        groupFetchResponse.setTopics(topics)
      }
    }
  }

  def handleFindCoordinatorRequest(request: RequestChannel.Request): Unit = {
    val version = request.header.apiVersion
    if (version < 4) {
      handleFindCoordinatorRequestLessThanV4(request)
    } else {
      handleFindCoordinatorRequestV4AndAbove(request)
    }
  }

  private def handleFindCoordinatorRequestV4AndAbove(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val coordinators = findCoordinatorRequest.data.coordinatorKeys.asScala.map { key =>
      val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, key)
      new FindCoordinatorResponseData.Coordinator()
        .setKey(key)
        .setErrorCode(error.code)
        .setHost(node.host)
        .setNodeId(node.id)
        .setPort(node.port)
    }
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val response = new FindCoordinatorResponse(
              new FindCoordinatorResponseData()
                .setCoordinators(coordinators.asJava)
                .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
              .format(response, request.header.correlationId, request.header.clientId))
      response
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponse)
  }

  private def handleFindCoordinatorRequestLessThanV4(request: RequestChannel.Request): Unit = {
    val findCoordinatorRequest = request.body[FindCoordinatorRequest]

    val (error, node) = getCoordinator(request, findCoordinatorRequest.data.keyType, findCoordinatorRequest.data.key)
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      val responseBody = new FindCoordinatorResponse(
          new FindCoordinatorResponseData()
            .setErrorCode(error.code)
            .setErrorMessage(error.message())
            .setNodeId(node.id)
            .setHost(node.host)
            .setPort(node.port)
            .setThrottleTimeMs(requestThrottleMs))
      trace("Sending FindCoordinator response %s for correlation id %d to client %s."
        .format(responseBody, request.header.correlationId, request.header.clientId))
      responseBody
    }
    if (error == Errors.NONE) {
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    } else {
      requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  private def getCoordinator(request: RequestChannel.Request, keyType: Byte, key: String): (Errors, Node) = {
    if (keyType == CoordinatorType.GROUP.id &&
        !authHelper.authorize(request.context, DESCRIBE, GROUP, key))
      (Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.TRANSACTION.id &&
        !authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, key))
      (Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, Node.noNode)
    else if (keyType == CoordinatorType.SHARE.id && request.context.apiVersion < 6)
      (Errors.INVALID_REQUEST, Node.noNode)
    else {
      if (keyType == CoordinatorType.SHARE.id) {
        authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
        try {
          SharePartitionKey.validate(key)
        } catch {
          case e: IllegalArgumentException =>
            error(s"Share coordinator key is invalid", e)
            return (Errors.INVALID_REQUEST, Node.noNode)
        }
      }
      val (partition, internalTopicName) = CoordinatorType.forId(keyType) match {
        case CoordinatorType.GROUP =>
          (groupCoordinator.partitionFor(key), GROUP_METADATA_TOPIC_NAME)

        case CoordinatorType.TRANSACTION =>
          (txnCoordinator.partitionFor(key), TRANSACTION_STATE_TOPIC_NAME)

        case CoordinatorType.SHARE =>
          // We know that shareCoordinator is defined at this stage.
          (shareCoordinator.partitionFor(SharePartitionKey.getInstance(key)), SHARE_GROUP_STATE_TOPIC_NAME)
      }

      val topicMetadata = metadataCache.getTopicMetadata(Set(internalTopicName).asJava, request.context.listenerName, false, false).asScala

      if (topicMetadata.headOption.isEmpty) {
        val controllerMutationQuota = quotas.controllerMutation.newPermissiveQuotaFor(request.session, request.header.clientId)
        autoTopicCreationManager.createTopics(Seq(internalTopicName).toSet, controllerMutationQuota, None)
        (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
      } else {
        if (topicMetadata.head.errorCode != Errors.NONE.code) {
          (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
        } else {
          val coordinatorEndpoint = topicMetadata.head.partitions.stream()
            .filter(_.partitionIndex() == partition)
            .filter(_.leaderId != MetadataResponse.NO_LEADER_ID)
            .flatMap(metadata => metadataCache.getAliveBrokerNode(metadata.leaderId, request.context.listenerName).stream())
            .findFirst()

          if (coordinatorEndpoint.isPresent) {
            (Errors.NONE, coordinatorEndpoint.get)
          } else {
            (Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
          }
        }
      }
    }
  }

  def handleDescribeGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val describeRequest = request.body[DescribeGroupsRequest]
    val includeAuthorizedOperations = describeRequest.data.includeAuthorizedOperations
    val response = new DescribeGroupsResponseData()
    val authorizedGroups = new ArrayBuffer[String]()

    describeRequest.data.groups.forEach { groupId =>
      if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
        response.groups.add(DescribeGroupsResponse.groupError(
          groupId,
          Errors.GROUP_AUTHORIZATION_FAILED
        ))
      } else {
        authorizedGroups += groupId
      }
    }

    groupCoordinator.describeGroups(
      request.context,
      authorizedGroups.asJava
    ).handle[Unit] { (results, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, describeRequest.getErrorResponse(exception))
      } else {
        if (request.header.apiVersion >= 3 && includeAuthorizedOperations) {
          results.forEach { groupResult =>
            if (groupResult.errorCode == Errors.NONE.code) {
              groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                request,
                new Resource(ResourceType.GROUP, groupResult.groupId)
              ))
            }
          }
        }

        if (response.groups.isEmpty) {
          // If the response is empty, we can directly reuse the results.
          response.setGroups(results)
        } else {
          // Otherwise, we have to copy the results into the existing ones.
          response.groups.addAll(results)
        }

        requestHelper.sendMaybeThrottle(request, new DescribeGroupsResponse(response))
      }
    }
  }

  def handleListGroupsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val listGroupsRequest = request.body[ListGroupsRequest]
    val hasClusterDescribe = authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME, logIfDenied = false)

    groupCoordinator.listGroups(
      request.context,
      listGroupsRequest.data
    ).handle[Unit] { (response, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, listGroupsRequest.getErrorResponse(exception))
      } else {
        val listGroupsResponse = if (hasClusterDescribe) {
          // With describe cluster access all groups are returned. We keep this alternative for backward compatibility.
          new ListGroupsResponse(response)
        } else {
          // Otherwise, only groups with described group are returned.
          val authorizedGroups = response.groups.asScala.filter { group =>
            authHelper.authorize(request.context, DESCRIBE, GROUP, group.groupId, logIfDenied = false)
          }
          new ListGroupsResponse(response.setGroups(authorizedGroups.asJava))
        }
        requestHelper.sendMaybeThrottle(request, listGroupsResponse)
      }
    }
  }

  def handleJoinGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val joinGroupRequest = request.body[JoinGroupRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, joinGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.joinGroup(
        request.context,
        joinGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, joinGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new JoinGroupResponse(response, request.context.apiVersion))
        }
      }
    }
  }

  def handleSyncGroupRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val syncGroupRequest = request.body[SyncGroupRequest]

    if (!syncGroupRequest.areMandatoryProtocolTypeAndNamePresent()) {
      // Starting from version 5, ProtocolType and ProtocolName fields are mandatory.
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.INCONSISTENT_GROUP_PROTOCOL.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, syncGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.syncGroup(
        request.context,
        syncGroupRequest.data,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, syncGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new SyncGroupResponse(response))
        }
      }
    }
  }

  def handleDeleteGroupsRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val deleteGroupsRequest = request.body[DeleteGroupsRequest]
    val groups = deleteGroupsRequest.data.groupsNames.asScala.distinct

    val (authorizedGroups, unauthorizedGroups) =
      authHelper.partitionSeqByAuthorized(request.context, DELETE, GROUP, groups)(identity)

    groupCoordinator.deleteGroups(
      request.context,
      authorizedGroups.toList.asJava,
      requestLocal.bufferSupplier
    ).handle[Unit] { (results, exception) =>
      val response = new DeleteGroupsResponseData()

      if (exception != null) {
        val error = Errors.forException(exception)
        authorizedGroups.foreach { groupId =>
          response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
            .setGroupId(groupId)
            .setErrorCode(error.code))
        }
      } else {
        response.setResults(results)
      }

      unauthorizedGroups.foreach { groupId =>
        response.results.add(new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId(groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      }

      requestHelper.sendMaybeThrottle(request, new DeleteGroupsResponse(response))
    }
  }

  def handleHeartbeatRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val heartbeatRequest = request.body[HeartbeatRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, heartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.heartbeat(
        request.context,
        heartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, heartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new HeartbeatResponse(response))
        }
      }
    }
  }

  def handleLeaveGroupRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val leaveGroupRequest = request.body[LeaveGroupRequest]

    if (!authHelper.authorize(request.context, READ, GROUP, leaveGroupRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      groupCoordinator.leaveGroup(
        request.context,
        leaveGroupRequest.normalizedData()
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, leaveGroupRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new LeaveGroupResponse(response, leaveGroupRequest.version))
        }
      }
    }
  }

  def handleSaslHandshakeRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslHandshakeResponse(responseData))
  }

  def handleSaslAuthenticateRequest(request: RequestChannel.Request): Unit = {
    val responseData = new SaslAuthenticateResponseData()
      .setErrorCode(Errors.ILLEGAL_SASL_STATE.code)
      .setErrorMessage("SaslAuthenticate request received after successful authentication")
    requestHelper.sendResponseMaybeThrottle(request, _ => new SaslAuthenticateResponse(responseData))
  }

  def handleApiVersionsRequest(request: RequestChannel.Request): Unit = {
    // Note that broker returns its full list of supported ApiKeys and versions regardless of current
    // authentication state (e.g., before SASL authentication on an SASL listener, do note that no
    // Kafka protocol requests may take place on an SSL listener before the SSL handshake is finished).
    // If this is considered to leak information about the broker version a workaround is to use SSL
    // with client authentication which is performed at an earlier stage of the connection where the
    // ApiVersionRequest is not available.
    def createResponseCallback(requestThrottleMs: Int): ApiVersionsResponse = {
      val apiVersionRequest = request.body[ApiVersionsRequest]
      if (apiVersionRequest.hasUnsupportedRequestVersion) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.UNSUPPORTED_VERSION.exception)
      } else if (!apiVersionRequest.isValid) {
        apiVersionRequest.getErrorResponse(requestThrottleMs, Errors.INVALID_REQUEST.exception)
      } else {
        apiVersionManager.apiVersionResponse(requestThrottleMs, request.header.apiVersion() < 4)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, createResponseCallback)
  }

  def handleDeleteRecordsRequest(request: RequestChannel.Request): Unit = {
    val deleteRecordsRequest = request.body[DeleteRecordsRequest]

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, DeleteRecordsPartitionResult]()
    val authorizedForDeleteTopicOffsets = mutable.Map[TopicPartition, Long]()

    val topics = deleteRecordsRequest.data.topics.asScala
    val authorizedTopics = authHelper.filterByAuthorized(request.context, DELETE, TOPIC, topics)(_.name)
    val deleteTopicPartitions = topics.flatMap { deleteTopic =>
      deleteTopic.partitions.asScala.map { deletePartition =>
        new TopicPartition(deleteTopic.name, deletePartition.partitionIndex) -> deletePartition.offset
      }
    }
    for ((topicPartition, offset) <- deleteTopicPartitions) {
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new DeleteRecordsPartitionResult()
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      else
        authorizedForDeleteTopicOffsets += (topicPartition -> offset)
    }

    // the callback for sending a DeleteRecordsResponse
    def sendResponseCallback(authorizedTopicResponses: Map[TopicPartition, DeleteRecordsPartitionResult]): Unit = {
      val mergedResponseStatus = authorizedTopicResponses ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      mergedResponseStatus.foreachEntry { (topicPartition, status) =>
        if (status.errorCode != Errors.NONE.code) {
          debug("DeleteRecordsRequest with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            Errors.forCode(status.errorCode).exceptionName))
        }
      }

      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DeleteRecordsResponse(new DeleteRecordsResponseData()
          .setThrottleTimeMs(requestThrottleMs)
          .setTopics(new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(mergedResponseStatus.groupBy(_._1.topic).map { case (topic, partitionMap) =>
            new DeleteRecordsTopicResult()
              .setName(topic)
              .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(partitionMap.map { case (topicPartition, partitionResult) =>
                new DeleteRecordsPartitionResult().setPartitionIndex(topicPartition.partition)
                  .setLowWatermark(partitionResult.lowWatermark)
                  .setErrorCode(partitionResult.errorCode)
              }.toList.asJava.iterator()))
          }.toList.asJava.iterator()))))
    }

    if (authorizedForDeleteTopicOffsets.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // call the replica manager to append messages to the replicas
      replicaManager.deleteRecords(
        deleteRecordsRequest.data.timeoutMs.toLong,
        authorizedForDeleteTopicOffsets,
        sendResponseCallback)
    }
  }

  def handleInitProducerIdRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.data.transactionalId

    if (transactionalId != null) {
      if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
      if (initProducerIdRequest.enable2Pc() && !authHelper.authorize(request.context, TWO_PHASE_COMMIT, TRANSACTIONAL_ID, transactionalId)) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    } else if (!authHelper.authorize(request.context, IDEMPOTENT_WRITE, CLUSTER, CLUSTER_NAME, true, false)
        && !authHelper.authorizeByResourceType(request.context, AclOperation.WRITE, ResourceType.TOPIC)) {
      requestHelper.sendErrorResponseMaybeThrottle(request, Errors.CLUSTER_AUTHORIZATION_FAILED.exception)
      return
    }

    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val finalError =
          if (initProducerIdRequest.version < 4 && result.error == Errors.PRODUCER_FENCED) {
            // For older clients, they could not understand the new PRODUCER_FENCED error code,
            // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
            Errors.INVALID_PRODUCER_EPOCH
          } else {
            result.error
          }
        val responseData = new InitProducerIdResponseData()
          .setProducerId(result.producerId)
          .setProducerEpoch(result.producerEpoch)
          .setThrottleTimeMs(requestThrottleMs)
          .setErrorCode(finalError.code)
        val responseBody = new InitProducerIdResponse(responseData)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      requestHelper.sendResponseMaybeThrottle(request, createResponse)
    }

    val producerIdAndEpoch = (initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch) match {
      case (RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH) => Right(None)
      case (RecordBatch.NO_PRODUCER_ID, _) | (_, RecordBatch.NO_PRODUCER_EPOCH) => Left(Errors.INVALID_REQUEST)
      case (_, _) => Right(Some(new ProducerIdAndEpoch(initProducerIdRequest.data.producerId, initProducerIdRequest.data.producerEpoch)))
    }

    producerIdAndEpoch match {
      case Right(producerIdAndEpoch) =>
        val enableTwoPC = initProducerIdRequest.enable2Pc()
        val keepPreparedTxn = initProducerIdRequest.keepPreparedTxn()

        txnCoordinator.handleInitProducerId(
            transactionalId,
            initProducerIdRequest.data.transactionTimeoutMs,
            enableTwoPC,
            keepPreparedTxn,
            producerIdAndEpoch,
            sendResponseCallback,
            requestLocal
        )
      case Left(error) => requestHelper.sendErrorResponseMaybeThrottle(request, error.exception)
    }
  }

  def handleEndTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.data.transactionalId

    if (authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
      def sendResponseCallback(error: Errors, newProducerId: Long, newProducerEpoch: Short): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (endTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }
          val responseBody = new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(finalError.code)
            .setProducerId(newProducerId)
            .setProducerEpoch(newProducerEpoch)
            .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed ${endTxnRequest.data.transactionalId}'s EndTxnRequest " +
            s"with committed: ${endTxnRequest.data.committed}, " +
            s"errors: $error from client ${request.header.clientId}.")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.data.transactionalId,
        endTxnRequest.data.producerId,
        endTxnRequest.data.producerEpoch,
        endTxnRequest.result(),
        TransactionVersion.transactionVersionForEndTxn(endTxnRequest),
        sendResponseCallback,
        requestLocal)
    } else
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(new EndTxnResponseData()
            .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
            .setThrottleTimeMs(requestThrottleMs))
      )
  }

  def handleWriteTxnMarkersRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    // We are checking for AlterCluster permissions first. If it is not present, we are authorizing cluster operation
    // The latter will throw an exception if it is denied.
    if (!authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME, logIfDenied = false)) {
      authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    }
    val writeTxnMarkersRequest = request.body[WriteTxnMarkersRequest]
    val errors = new ConcurrentHashMap[java.lang.Long, util.Map[TopicPartition, Errors]]()
    val markers = writeTxnMarkersRequest.markers
    val numAppends = new AtomicInteger(markers.size)

    if (numAppends.get == 0) {
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
      return
    }

    def updateErrors(producerId: Long, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      val previousErrors = errors.putIfAbsent(producerId, currentErrors)
      if (previousErrors != null)
        previousErrors.putAll(currentErrors)
    }

    /**
      * This is the call back invoked when a log append of transaction markers succeeds. This can be called multiple
      * times when handling a single WriteTxnMarkersRequest because there is one append per TransactionMarker in the
      * request, so there could be multiple appends of markers to the log. The final response will be sent only
      * after all appends have returned.
      */
    def maybeSendResponseCallback(producerId: Long, result: TransactionResult, currentErrors: ConcurrentHashMap[TopicPartition, Errors]): Unit = {
      trace(s"End transaction marker append for producer id $producerId completed with status: $currentErrors")
      updateErrors(producerId, currentErrors)

      if (numAppends.decrementAndGet() == 0) {
        requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
      }
    }

    // TODO: The current append API makes doing separate writes per producerId a little easier, but it would
    // be nice to have only one append to the log. This requires pushing the building of the control records
    // into Log so that we only append those having a valid producer epoch, and exposing a new appendControlRecord
    // API in ReplicaManager. For now, we've done the simpler approach
    var skippedMarkers = 0
    for (marker <- markers.asScala) {
      val producerId = marker.producerId
      val partitionsWithCompatibleMessageFormat = new mutable.ArrayBuffer[TopicPartition]

      val currentErrors = new ConcurrentHashMap[TopicPartition, Errors]()
      marker.partitions.forEach { partition =>
        replicaManager.onlinePartition(partition) match {
          case Some(_)  =>
            partitionsWithCompatibleMessageFormat += partition
          case None =>
            currentErrors.put(partition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
      }

      if (!currentErrors.isEmpty)
        updateErrors(producerId, currentErrors)

      if (partitionsWithCompatibleMessageFormat.isEmpty) {
        numAppends.decrementAndGet()
        skippedMarkers += 1
      } else {
        val controlRecordType = marker.transactionResult match {
          case TransactionResult.COMMIT => ControlRecordType.COMMIT
          case TransactionResult.ABORT => ControlRecordType.ABORT
        }

        val markerResults = new ConcurrentHashMap[TopicPartition, Errors]()
        val numPartitions = new AtomicInteger(partitionsWithCompatibleMessageFormat.size)
        def addResultAndMaybeComplete(partition: TopicPartition, error: Errors): Unit = {
          markerResults.put(partition, error)
          // We should only call maybeSendResponseCallback once per marker. Otherwise, it causes sending the response
          // prematurely.
          if (numPartitions.decrementAndGet() == 0) {
            maybeSendResponseCallback(producerId, marker.transactionResult, markerResults)
          }
        }

        val controlRecords = mutable.Map.empty[TopicIdPartition, MemoryRecords]
        partitionsWithCompatibleMessageFormat.foreach { partition =>
          if (partition.topic == GROUP_METADATA_TOPIC_NAME) {
            groupCoordinator.completeTransaction(
              partition,
              marker.producerId,
              marker.producerEpoch,
              marker.coordinatorEpoch,
              marker.transactionResult,
              Duration.ofMillis(config.requestTimeoutMs.toLong)
            ).whenComplete { (_, exception) =>
              val error = if (exception == null) {
                Errors.NONE
              } else {
                Errors.forException(exception) match {
                  case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
                    // The transaction coordinator does not expect those errors so we translate them
                    // to NOT_LEADER_OR_FOLLOWER to signal to it that the coordinator is not ready yet.
                    Errors.NOT_LEADER_OR_FOLLOWER
                  case error =>
                    error
                }
              }
              addResultAndMaybeComplete(partition, error)
            }
          } else {
            // Otherwise, the regular appendRecords path is used for all the non __consumer_offsets
            // partitions or for all partitions when the new group coordinator is disabled.
            // If topicIdPartition contains Uuid.ZERO_UUid all functionality will fall back on topic name.
            controlRecords += replicaManager.topicIdPartition(partition) -> MemoryRecords.withEndTransactionMarker(
              producerId,
              marker.producerEpoch,
              new EndTransactionMarker(controlRecordType, marker.coordinatorEpoch)
            )
          }
        }

        if (controlRecords.nonEmpty) {
          replicaManager.appendRecords(
            timeout = config.requestTimeoutMs.toLong,
            requiredAcks = -1,
            internalTopicsAllowed = true,
            origin = AppendOrigin.COORDINATOR,
            entriesPerPartition = controlRecords,
            requestLocal = requestLocal,
            responseCallback = errors => {
              errors.foreachEntry { (topicIdPartition, partitionResponse) =>
                addResultAndMaybeComplete(topicIdPartition.topicPartition(), partitionResponse.error)
              }
            }
          )
        }
      }
    }

    // No log appends were written as all partitions had incorrect log format
    // so we need to send the error response
    if (skippedMarkers == markers.size)
      requestHelper.sendResponseExemptThrottle(request, new WriteTxnMarkersResponse(errors))
  }

  def handleAddPartitionsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val addPartitionsToTxnRequest =
      if (request.context.apiVersion() < 4)
        request.body[AddPartitionsToTxnRequest].normalizeRequest()
      else
        request.body[AddPartitionsToTxnRequest]
    val version = addPartitionsToTxnRequest.version
    val responses = new AddPartitionsToTxnResultCollection()
    val partitionsByTransaction = addPartitionsToTxnRequest.partitionsByTransaction()

    // Newer versions of the request should only come from other brokers.
    if (version >= 4) authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)

    // V4 requests introduced batches of transactions. We need all transactions to be handled before sending the
    // response so there are a few differences in handling errors and sending responses.
    def createResponse(requestThrottleMs: Int): AbstractResponse = {
      if (version < 4) {
        // There will only be one response in data. Add it to the response data object.
        val data = new AddPartitionsToTxnResponseData()
        responses.forEach { result =>
          data.setResultsByTopicV3AndBelow(result.topicResults())
          data.setThrottleTimeMs(requestThrottleMs)
        }
        new AddPartitionsToTxnResponse(data)
      } else {
        new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData().setThrottleTimeMs(requestThrottleMs).setResultsByTransaction(responses))
      }
    }

    val txns = addPartitionsToTxnRequest.data.transactions
    def addResultAndMaybeSendResponse(result: AddPartitionsToTxnResult): Unit = {
      val canSend = responses.synchronized {
        responses.add(result)
        responses.size == txns.size
      }
      if (canSend) {
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }
    }

    txns.forEach { transaction =>
      val transactionalId = transaction.transactionalId

      if (transactionalId == null)
        throw new InvalidRequestException("Transactional ID can not be null in request.")

      val partitionsToAdd = partitionsByTransaction.get(transactionalId).asScala

      // Versions < 4 come from clients and must be authorized to write for the given transaction and for the given topics.
      if (version < 4 && !authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId)) {
        addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
      } else {
        val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
        val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
        val authorizedPartitions = mutable.Set[TopicPartition]()

        // Only request versions less than 4 need write authorization since they come from clients.
        val authorizedTopics =
          if (version < 4)
            authHelper.filterByAuthorized(request.context, WRITE, TOPIC, partitionsToAdd.filterNot(tp => Topic.isInternal(tp.topic)))(_.topic)
          else
            partitionsToAdd.map(_.topic).toSet
        for (topicPartition <- partitionsToAdd) {
          if (!authorizedTopics.contains(topicPartition.topic))
            unauthorizedTopicErrors += topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED
          else if (!metadataCache.contains(topicPartition))
            nonExistingTopicErrors += topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION
          else
            authorizedPartitions.add(topicPartition)
        }

        if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
          // Any failed partition check causes the entire transaction to fail. We send the appropriate error codes for the
          // partitions which failed, and an 'OPERATION_NOT_ATTEMPTED' error code for the partitions which succeeded
          // the authorization check to indicate that they were not added to the transaction.
          val partitionErrors = unauthorizedTopicErrors ++ nonExistingTopicErrors ++
            authorizedPartitions.map(_ -> Errors.OPERATION_NOT_ATTEMPTED)
          addResultAndMaybeSendResponse(AddPartitionsToTxnResponse.resultForTransaction(transactionalId, partitionErrors.asJava))
        } else {
          def sendResponseCallback(error: Errors): Unit = {
            val finalError = {
              if (version < 2 && error == Errors.PRODUCER_FENCED) {
                // For older clients, they could not understand the new PRODUCER_FENCED error code,
                // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
                Errors.INVALID_PRODUCER_EPOCH
              } else {
                error
              }
            }
            addResultAndMaybeSendResponse(addPartitionsToTxnRequest.errorResponseForTransaction(transactionalId, finalError))
          }

          if (!transaction.verifyOnly) {
            txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              sendResponseCallback,
              TransactionVersion.transactionVersionForAddPartitionsToTxn(addPartitionsToTxnRequest),
              requestLocal)
          } else {
            txnCoordinator.handleVerifyPartitionsInTransaction(transactionalId,
              transaction.producerId,
              transaction.producerEpoch,
              authorizedPartitions,
              addResultAndMaybeSendResponse)
          }
        }
      }
    }
  }

  def handleAddOffsetsToTxnRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val addOffsetsToTxnRequest = request.body[AddOffsetsToTxnRequest]
    val transactionalId = addOffsetsToTxnRequest.data.transactionalId
    val groupId = addOffsetsToTxnRequest.data.groupId
    val offsetTopicPartition = new TopicPartition(GROUP_METADATA_TOPIC_NAME, groupCoordinator.partitionFor(groupId))

    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, transactionalId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs)))
    else if (!authHelper.authorize(request.context, READ, GROUP, groupId))
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AddOffsetsToTxnResponse(new AddOffsetsToTxnResponseData()
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          .setThrottleTimeMs(requestThrottleMs))
      )
    else {
      def sendResponseCallback(error: Errors): Unit = {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val finalError =
            if (addOffsetsToTxnRequest.version < 2 && error == Errors.PRODUCER_FENCED) {
              // For older clients, they could not understand the new PRODUCER_FENCED error code,
              // so we need to return the old INVALID_PRODUCER_EPOCH to have the same client handling logic.
              Errors.INVALID_PRODUCER_EPOCH
            } else {
              error
            }

          val responseBody: AddOffsetsToTxnResponse = new AddOffsetsToTxnResponse(
            new AddOffsetsToTxnResponseData()
              .setErrorCode(finalError.code)
              .setThrottleTimeMs(requestThrottleMs))
          trace(s"Completed $transactionalId's AddOffsetsToTxnRequest for group $groupId on partition " +
            s"$offsetTopicPartition: errors: $error from client ${request.header.clientId}")
          responseBody
        }
        requestHelper.sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
        addOffsetsToTxnRequest.data.producerId,
        addOffsetsToTxnRequest.data.producerEpoch,
        Set(offsetTopicPartition),
        sendResponseCallback,
        TransactionVersion.TV_0, // This request will always come from the client not using TV 2.
        requestLocal)
    }
  }

  def handleTxnOffsetCommitRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val txnOffsetCommitRequest = request.body[TxnOffsetCommitRequest]

    def sendResponse(response: TxnOffsetCommitResponse): Unit = {
      // We need to replace COORDINATOR_LOAD_IN_PROGRESS with COORDINATOR_NOT_AVAILABLE
      // for older producer client from 0.11 to prior 2.0, which could potentially crash due
      // to unexpected loading error. This bug is fixed later by KAFKA-7296. Clients using
      // txn commit protocol >= 2 (version 2.3 and onwards) are guaranteed to have
      // the fix to check for the loading error.
      if (txnOffsetCommitRequest.version < 2) {
        response.data.topics.forEach { topic =>
          topic.partitions.forEach { partition =>
            if (partition.errorCode == Errors.COORDINATOR_LOAD_IN_PROGRESS.code) {
              partition.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)
            }
          }
        }
      }

      requestHelper.sendMaybeThrottle(request, response)
    }

    // authorize for the transactionalId and the consumer group. Note that we skip producerId authorization
    // since it is implied by transactionalId authorization
    if (!authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, txnOffsetCommitRequest.data.transactionalId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, txnOffsetCommitRequest.data.groupId)) {
      sendResponse(txnOffsetCommitRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        txnOffsetCommitRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new TxnOffsetCommitResponse.Builder()
      val authorizedTopicCommittedOffsets = new mutable.ArrayBuffer[TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic]()
      txnOffsetCommitRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).isPresent()) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicCommittedOffsets += topicWithValidPartitions
          }
        }
      }

      if (authorizedTopicCommittedOffsets.isEmpty) {
        sendResponse(responseBuilder.build())
        CompletableFuture.completedFuture[Unit](())
      } else {
        val txnOffsetCommitRequestData = new TxnOffsetCommitRequestData()
          .setGroupId(txnOffsetCommitRequest.data.groupId)
          .setMemberId(txnOffsetCommitRequest.data.memberId)
          .setGenerationId(txnOffsetCommitRequest.data.generationId)
          .setGroupInstanceId(txnOffsetCommitRequest.data.groupInstanceId)
          .setProducerEpoch(txnOffsetCommitRequest.data.producerEpoch)
          .setProducerId(txnOffsetCommitRequest.data.producerId)
          .setTransactionalId(txnOffsetCommitRequest.data.transactionalId)
          .setTopics(authorizedTopicCommittedOffsets.asJava)

        groupCoordinator.commitTransactionalOffsets(
          request.context,
          txnOffsetCommitRequestData,
          requestLocal.bufferSupplier
        ).handle[Unit] { (response, exception) =>
          if (exception != null) {
            sendResponse(txnOffsetCommitRequest.getErrorResponse(exception))
          } else {
            sendResponse(responseBuilder.merge(response).build())
          }
        }
      }
    }
  }

  def handleDescribeAcls(request: RequestChannel.Request): Unit = {
    aclApis.handleDescribeAcls(request)
  }

  def handleOffsetForLeaderEpochRequest(request: RequestChannel.Request): Unit = {
    val offsetForLeaderEpoch = request.body[OffsetsForLeaderEpochRequest]
    val topics = offsetForLeaderEpoch.data.topics.asScala.toSeq

    // The OffsetsForLeaderEpoch API was initially only used for inter-broker communication and required
    // cluster permission. With KIP-320, the consumer now also uses this API to check for log truncation
    // following a leader change, so we also allow topic describe permission.
    val (authorizedTopics, unauthorizedTopics) =
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME, logIfDenied = false))
        (topics, Seq.empty[OffsetForLeaderTopic])
      else authHelper.partitionSeqByAuthorized(request.context, DESCRIBE, TOPIC, topics)(_.topic)

    val endOffsetsForAuthorizedPartitions = replicaManager.lastOffsetForLeaderEpoch(authorizedTopics)
    val endOffsetsForUnauthorizedPartitions = unauthorizedTopics.map { offsetForLeaderTopic =>
      val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
        new EpochEndOffset()
          .setPartition(offsetForLeaderPartition.partition)
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
      }

      new OffsetForLeaderTopicResult()
        .setTopic(offsetForLeaderTopic.topic)
        .setPartitions(partitions.toList.asJava)
    }

    val endOffsetsForAllTopics = new OffsetForLeaderTopicResultCollection(
      (endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions).asJava.iterator
    )

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new OffsetsForLeaderEpochResponse(new OffsetForLeaderEpochResponseData()
        .setThrottleTimeMs(requestThrottleMs)
        .setTopics(endOffsetsForAllTopics)))
  }

  def handleAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[AlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data())
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)
    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: AlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new AlterConfigsResponse(ConfigAdminManager.reassembleLegacyResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }
    if (remaining.resources().isEmpty) {
      sendResponse(Some(new AlterConfigsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new AlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    }
  }

  def handleIncrementalAlterConfigsRequest(request: RequestChannel.Request): Unit = {
    val original = request.body[IncrementalAlterConfigsRequest]
    val preprocessingResponses = configManager.preprocess(original.data(),
      (rType, rName) => authHelper.authorize(request.context, ALTER_CONFIGS, rType, rName))
    val remaining = ConfigAdminManager.copyWithoutPreprocessed(original.data(), preprocessingResponses)

    def sendResponse(secondPart: Option[ApiMessage]): Unit = {
      secondPart match {
        case Some(result: IncrementalAlterConfigsResponseData) =>
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
            new IncrementalAlterConfigsResponse(ConfigAdminManager.reassembleIncrementalResponse(
              original.data(),
              preprocessingResponses,
              result).setThrottleTimeMs(requestThrottleMs)))
        case _ => handleInvalidVersionsDuringForwarding(request)
      }
    }

    if (remaining.resources().isEmpty) {
      sendResponse(Some(new IncrementalAlterConfigsResponseData()))
    } else {
      forwardingManager.forwardRequest(request,
        new IncrementalAlterConfigsRequest(remaining, request.header.apiVersion()),
        response => sendResponse(response.map(_.data())))
    }
  }

  def handleDescribeConfigsRequest(request: RequestChannel.Request): Unit = {
    val responseData = configHelper.handleDescribeConfigsRequest(request, authHelper)
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeConfigsResponse(responseData.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleAlterReplicaLogDirsRequest(request: RequestChannel.Request): Unit = {
    val alterReplicaDirsRequest = request.body[AlterReplicaLogDirsRequest]
    if (authHelper.authorize(request.context, ALTER, CLUSTER, CLUSTER_NAME)) {
      val result = replicaManager.alterReplicaLogDirs(alterReplicaDirsRequest.partitionDirs.asScala)
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new AlterReplicaLogDirsResponse(new AlterReplicaLogDirsResponseData()
          .setResults(result.groupBy(_._1.topic).map {
            case (topic, errors) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult()
              .setTopicName(topic)
              .setPartitions(errors.map {
                case (tp, error) => new AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(error.code)
              }.toList.asJava)
          }.toList.asJava)
          .setThrottleTimeMs(requestThrottleMs)))
    } else {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        alterReplicaDirsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    }
  }

  def handleDescribeLogDirsRequest(request: RequestChannel.Request): Unit = {
    val describeLogDirsDirRequest = request.body[DescribeLogDirsRequest]
    val (logDirInfos, error) = {
      if (authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
        val partitions =
          if (describeLogDirsDirRequest.isAllTopicPartitions)
            replicaManager.logManager.allLogs.map(_.topicPartition).toSet
          else
            describeLogDirsDirRequest.data.topics.asScala.flatMap(
              logDirTopic => logDirTopic.partitions.asScala.map(partitionIndex =>
                new TopicPartition(logDirTopic.topic, partitionIndex))).toSet

        (replicaManager.describeLogDirs(partitions), Errors.NONE)
      } else {
        (util.Collections.emptyList[DescribeLogDirsResponseData.DescribeLogDirsResult], Errors.CLUSTER_AUTHORIZATION_FAILED)
      }
    }
    requestHelper.sendResponseMaybeThrottle(request, throttleTimeMs => new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
      .setThrottleTimeMs(throttleTimeMs)
      .setResults(logDirInfos)
      .setErrorCode(error.code)))
  }

  def handleCreateTokenRequest(request: RequestChannel.Request): Unit = {
    val createTokenRequest = request.body[CreateDelegationTokenRequest]

    val requester = request.context.principal
    val ownerPrincipalName = createTokenRequest.data.ownerPrincipalName
    val owner = if (ownerPrincipalName == null || ownerPrincipalName.isEmpty) {
      request.context.principal
    } else {
      new KafkaPrincipal(createTokenRequest.data.ownerPrincipalType, ownerPrincipalName)
    }
    val renewerList = createTokenRequest.data.renewers.asScala.toList.map(entry =>
      new KafkaPrincipal(entry.principalType, entry.principalName))

    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, owner, requester))
    } else if (!owner.equals(requester) && !authHelper.authorize(request.context, CREATE_TOKENS, USER, owner.toString)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED, owner, requester))
    } else if (renewerList.exists(principal => principal.getPrincipalType != KafkaPrincipal.USER_TYPE)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        CreateDelegationTokenResponse.prepareResponse(request.context.requestVersion, requestThrottleMs,
          Errors.INVALID_PRINCIPAL_TYPE, owner, requester))
    } else {
      forwardToController(request)
    }
  }

  def handleExpireTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new ExpireDelegationTokenResponse(
          new ExpireDelegationTokenResponseData()
              .setThrottleTimeMs(requestThrottleMs)
              .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
              .setExpiryTimestampMs(DelegationTokenManager.ERROR_TIMESTAMP)))
    } else {
      forwardToController(request)
    }
  }

  def handleRenewTokenRequest(request: RequestChannel.Request): Unit = {
    if (!allowTokenRequests(request)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new RenewDelegationTokenResponse(
          new RenewDelegationTokenResponseData()
            .setThrottleTimeMs(requestThrottleMs)
            .setErrorCode(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.code)
            .setExpiryTimestampMs(DelegationTokenManager.ERROR_TIMESTAMP)))
    } else {
      forwardToController(request)
    }
  }

  def handleDescribeTokensRequest(request: RequestChannel.Request): Unit = {
    val describeTokenRequest = request.body[DescribeDelegationTokenRequest]

    // the callback for sending a describe token response
    def sendResponseCallback(error: Errors, tokenDetails: util.List[DelegationToken]): Unit = {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeDelegationTokenResponse(request.context.requestVersion(), requestThrottleMs, error, tokenDetails))
      trace("Sending describe token response for correlation id %d to client %s."
        .format(request.header.correlationId, request.header.clientId))
    }

    if (!allowTokenRequests(request))
      sendResponseCallback(Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, Collections.emptyList)
    else if (!new DelegationTokenManagerConfigs(config).tokenAuthEnabled)
      sendResponseCallback(Errors.DELEGATION_TOKEN_AUTH_DISABLED, Collections.emptyList)
    else {
      val requestPrincipal = request.context.principal

      if (describeTokenRequest.ownersListEmpty()) {
        sendResponseCallback(Errors.NONE, Collections.emptyList)
      }
      else {
        val owners: Optional[util.List[KafkaPrincipal]] = if (describeTokenRequest.data.owners == null)
          Optional.empty()
        else
          Optional.of(describeTokenRequest.data.owners.stream().map(p => new KafkaPrincipal(p.principalType(), p.principalName)).toList)
        def authorizeToken(tokenId: String) = authHelper.authorize(request.context, DESCRIBE, DELEGATION_TOKEN, tokenId)
        def authorizeRequester(owner: KafkaPrincipal) = authHelper.authorize(request.context, DESCRIBE_TOKENS, USER, owner.toString)
        def eligible(token: TokenInformation) = DelegationTokenManager
          .filterToken(requestPrincipal, owners, token, authorizeToken, authorizeRequester)
        val tokens =  tokenManager.getTokens(eligible)
        sendResponseCallback(Errors.NONE, tokens)
      }
    }
  }

  def allowTokenRequests(request: RequestChannel.Request): Boolean = {
    val protocol = request.context.securityProtocol
    if (request.context.principal.tokenAuthenticated ||
      protocol == SecurityProtocol.PLAINTEXT ||
      // disallow requests from 1-way SSL
      (protocol == SecurityProtocol.SSL && request.context.principal == KafkaPrincipal.ANONYMOUS))
      false
    else
      true
  }

  def handleOffsetDeleteRequest(
    request: RequestChannel.Request,
    requestLocal: RequestLocal
  ): CompletableFuture[Unit] = {
    val offsetDeleteRequest = request.body[OffsetDeleteRequest]

    if (!authHelper.authorize(request.context, DELETE, GROUP, offsetDeleteRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val authorizedTopics = authHelper.filterByAuthorized(
        request.context,
        READ,
        TOPIC,
        offsetDeleteRequest.data.topics.asScala
      )(_.name)

      val responseBuilder = new OffsetDeleteResponse.Builder
      val authorizedTopicPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopicCollection()
      offsetDeleteRequest.data.topics.forEach { topic =>
        if (!authorizedTopics.contains(topic.name)) {
          // If the topic is not authorized, we add the topic and all its partitions
          // to the response with TOPIC_AUTHORIZATION_FAILED.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.TOPIC_AUTHORIZATION_FAILED)
        } else if (!metadataCache.contains(topic.name)) {
          // If the topic is unknown, we add the topic and all its partitions
          // to the response with UNKNOWN_TOPIC_OR_PARTITION.
          responseBuilder.addPartitions[OffsetDeleteRequestData.OffsetDeleteRequestPartition](
            topic.name, topic.partitions, _.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        } else {
          // Otherwise, we check all partitions to ensure that they all exist.
          val topicWithValidPartitions = new OffsetDeleteRequestData.OffsetDeleteRequestTopic().setName(topic.name)

          topic.partitions.forEach { partition =>
            if (metadataCache.getLeaderAndIsr(topic.name, partition.partitionIndex).isPresent) {
              topicWithValidPartitions.partitions.add(partition)
            } else {
              responseBuilder.addPartition(topic.name, partition.partitionIndex, Errors.UNKNOWN_TOPIC_OR_PARTITION)
            }
          }

          if (!topicWithValidPartitions.partitions.isEmpty) {
            authorizedTopicPartitions.add(topicWithValidPartitions)
          }
        }
      }

      val offsetDeleteRequestData = new OffsetDeleteRequestData()
        .setGroupId(offsetDeleteRequest.data.groupId)
        .setTopics(authorizedTopicPartitions)

      groupCoordinator.deleteOffsets(
        request.context,
        offsetDeleteRequestData,
        requestLocal.bufferSupplier
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, offsetDeleteRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, responseBuilder.merge(response).build())
        }
      }
    }
  }

  def handleDescribeClientQuotasRequest(request: RequestChannel.Request): Unit = {
    val describeClientQuotasRequest = request.body[DescribeClientQuotasRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeClientQuotasRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val result = metadataCache.describeClientQuotas(describeClientQuotasRequest.data())
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
        result.setThrottleTimeMs(requestThrottleMs)
        new DescribeClientQuotasResponse(result)
      })
    }
  }

  def handleDescribeUserScramCredentialsRequest(request: RequestChannel.Request): Unit = {
    val describeUserScramCredentialsRequest = request.body[DescribeUserScramCredentialsRequest]

    if (!authHelper.authorize(request.context, DESCRIBE, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        describeUserScramCredentialsRequest.getErrorResponse(requestThrottleMs, Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val result = metadataCache.describeScramCredentials(describeUserScramCredentialsRequest.data())
      requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
        new DescribeUserScramCredentialsResponse(result.setThrottleTimeMs(requestThrottleMs)))
    }
  }

  def handleDescribeCluster(request: RequestChannel.Request): Unit = {
    val response = authHelper.computeDescribeClusterResponse(
      request,
      EndpointType.BROKER,
      clusterId,
      () => {
        val brokers = new DescribeClusterResponseData.DescribeClusterBrokerCollection()
        val describeClusterRequest = request.body[DescribeClusterRequest]
        metadataCache.getBrokerNodes(request.context.listenerName).forEach { node =>
          if (!node.isFenced || describeClusterRequest.data().includeFencedBrokers()) {
          brokers.add(new DescribeClusterResponseData.DescribeClusterBroker().
            setBrokerId(node.id).
            setHost(node.host).
            setPort(node.port).
            setRack(node.rack).
            setIsFenced(node.isFenced))
          }
        }
        brokers
      },
      () => {
        metadataCache.getRandomAliveBrokerId.orElse(-1)
      }
    )
    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeClusterResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleDescribeProducersRequest(request: RequestChannel.Request): Unit = {
    val describeProducersRequest = request.body[DescribeProducersRequest]

    def partitionError(
      topicPartition: TopicPartition,
      apiError: ApiError
    ): DescribeProducersResponseData.PartitionResponse = {
      new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(topicPartition.partition)
        .setErrorCode(apiError.error.code)
        .setErrorMessage(apiError.message)
    }

    val response = new DescribeProducersResponseData()
    describeProducersRequest.data.topics.forEach { topicRequest =>
      val topicResponse = new DescribeProducersResponseData.TopicResponse()
        .setName(topicRequest.name)

      val invalidTopicError = checkValidTopic(topicRequest.name)

      val topicError = invalidTopicError.orElse {
        if (!authHelper.authorize(request.context, READ, TOPIC, topicRequest.name)) {
          Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
        } else if (!metadataCache.contains(topicRequest.name))
          Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
        else {
          None
        }
      }

      topicRequest.partitionIndexes.forEach { partitionId =>
        val topicPartition = new TopicPartition(topicRequest.name, partitionId)
        val partitionResponse = topicError match {
          case Some(error) => partitionError(topicPartition, error)
          case None => replicaManager.activeProducerState(topicPartition)
        }
        topicResponse.partitions.add(partitionResponse)
      }

      response.topics.add(topicResponse)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeProducersResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def checkValidTopic(topic: String): Option[ApiError] = {
    try {
      Topic.validate(topic)
      None
    } catch {
      case e: Throwable => Some(ApiError.fromThrowable(e))
    }
  }

  def handleDescribeTransactionsRequest(request: RequestChannel.Request): Unit = {
    val describeTransactionsRequest = request.body[DescribeTransactionsRequest]
    val response = new DescribeTransactionsResponseData()

    describeTransactionsRequest.data.transactionalIds.forEach { transactionalId =>
      val transactionState = if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionalId)) {
        new DescribeTransactionsResponseData.TransactionState()
          .setTransactionalId(transactionalId)
          .setErrorCode(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code)
      } else {
        txnCoordinator.handleDescribeTransactions(transactionalId)
      }

      // Include only partitions which the principal is authorized to describe
      val topicIter = transactionState.topics.iterator()
      while (topicIter.hasNext) {
        val topic = topicIter.next().topic
        if (!authHelper.authorize(request.context, DESCRIBE, TOPIC, topic)) {
          topicIter.remove()
        }
      }
      response.transactionStates.add(transactionState)
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new DescribeTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  def handleListTransactionsRequest(request: RequestChannel.Request): Unit = {
    val listTransactionsRequest = request.body[ListTransactionsRequest]
    val filteredProducerIds = listTransactionsRequest.data.producerIdFilters.asScala.map(Long.unbox).toSet
    val filteredStates = listTransactionsRequest.data.stateFilters.asScala.toSet
    val durationFilter = listTransactionsRequest.data.durationFilter()
    val transactionalIdPatternFilter = listTransactionsRequest.data.transactionalIdPattern
    val response = txnCoordinator.handleListTransactions(
      filteredProducerIds,
      filteredStates,
      durationFilter,
      transactionalIdPatternFilter
    )

    // The response should contain only transactionalIds that the principal
    // has `Describe` permission to access.
    val transactionStateIter = response.transactionStates.iterator()
    while (transactionStateIter.hasNext) {
      val transactionState = transactionStateIter.next()
      if (!authHelper.authorize(request.context, DESCRIBE, TRANSACTIONAL_ID, transactionState.transactionalId)) {
        transactionStateIter.remove()
      }
    }

    requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs =>
      new ListTransactionsResponse(response.setThrottleTimeMs(requestThrottleMs)))
  }

  private def groupVersion(): GroupVersion = {
    GroupVersion.fromFeatureLevel(metadataCache.features.finalizedFeatures.getOrDefault(GroupVersion.FEATURE_NAME, 0.toShort))
  }

  def isConsumerGroupProtocolEnabled(): Boolean = {
      config.groupCoordinatorRebalanceProtocols.contains(Group.GroupType.CONSUMER) &&
      groupVersion().isConsumerRebalanceProtocolSupported
  }

  def handleConsumerGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupHeartbeatRequest = request.body[ConsumerGroupHeartbeatRequest]

    if (!isConsumerGroupProtocolEnabled()) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, consumerGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      if (consumerGroupHeartbeatRequest.data.subscribedTopicNames != null &&
        !consumerGroupHeartbeatRequest.data.subscribedTopicNames.isEmpty) {
        // Check the authorization if the subscribed topic names are provided.
        // Clients are not allowed to see topics that are not authorized for Describe.
        val subscribedTopicSet = consumerGroupHeartbeatRequest.data.subscribedTopicNames.asScala.toSet
        val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
          subscribedTopicSet)(identity)
        if (authorizedTopics.size < subscribedTopicSet.size) {
          val responseData = new ConsumerGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
      }

      groupCoordinator.consumerGroupHeartbeat(
        request.context,
        consumerGroupHeartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ConsumerGroupHeartbeatResponse(response))
        }
      }
    }
  }

  def handleConsumerGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val consumerGroupDescribeRequest = request.body[ConsumerGroupDescribeRequest]
    val includeAuthorizedOperations = consumerGroupDescribeRequest.data.includeAuthorizedOperations

    if (!isConsumerGroupProtocolEnabled()) {
      // The API is not supported by the "old" group coordinator (the default). If the
      // new one is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, request.body[ConsumerGroupDescribeRequest].getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new ConsumerGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      consumerGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new ConsumerGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.consumerGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, consumerGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (includeAuthorizedOperations) {
            results.forEach { groupResult =>
              if (groupResult.errorCode == Errors.NONE.code) {
                groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                  request,
                  new Resource(ResourceType.GROUP, groupResult.groupId)
                ))
              }
            }
          }

          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          // Clients are not allowed to see topics that are not authorized for Describe.
          if (authorizerPlugin.isDefined) {
            val topicsToCheck = response.groups.stream()
              .flatMap(group => group.members.stream)
              .flatMap(member => util.stream.Stream.of(member.assignment, member.targetAssignment))
              .flatMap(assignment => assignment.topicPartitions.stream)
              .map(topicPartition => topicPartition.topicName)
              .collect(Collectors.toSet[String])
              .asScala
            val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
              topicsToCheck)(identity)
            val updatedGroups = response.groups.stream().map { group =>
              val hasUnauthorizedTopic = group.members.stream()
                .flatMap(member => util.stream.Stream.of(member.assignment, member.targetAssignment))
                .flatMap(assignment => assignment.topicPartitions.stream())
                .anyMatch(tp => !authorizedTopics.contains(tp.topicName))

              if (hasUnauthorizedTopic) {
                new ConsumerGroupDescribeResponseData.DescribedGroup()
                  .setGroupId(group.groupId)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setErrorMessage("The group has described topic(s) that the client is not authorized to describe.")
                  .setMembers(List.empty.asJava)
              } else {
                group
              }
            }.collect(Collectors.toList[ConsumerGroupDescribeResponseData.DescribedGroup])
            response.setGroups(updatedGroups)
          }

          requestHelper.sendMaybeThrottle(request, new ConsumerGroupDescribeResponse(response))
        }
      }
    }
  }

  private def streamsVersion(): StreamsVersion = {
    StreamsVersion.fromFeatureLevel(metadataCache.features.finalizedFeatures.getOrDefault(StreamsVersion.FEATURE_NAME, 0.toShort))
  }

  private def isStreamsGroupProtocolEnabled: Boolean = {
      config.groupCoordinatorRebalanceProtocols.contains(Group.GroupType.STREAMS) &&
      streamsVersion().streamsGroupSupported
  }

  def handleStreamsGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val streamsGroupHeartbeatRequest = request.body[StreamsGroupHeartbeatRequest]

    if (!isStreamsGroupProtocolEnabled) {
      // The API is not enabled by default. If it is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, streamsGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, streamsGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, streamsGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val requestContext = request.context

      if (streamsGroupHeartbeatRequest.data().topology() != null) {
        val requiredTopics: Seq[String] =
          streamsGroupHeartbeatRequest.data().topology().subtopologies().iterator().asScala.flatMap(subtopology =>
            (subtopology.sourceTopics().iterator().asScala:Iterator[String])
              ++ (subtopology.repartitionSinkTopics().iterator().asScala:Iterator[String])
              ++ (subtopology.repartitionSourceTopics().iterator().asScala.map(_.name()):Iterator[String])
              ++ (subtopology.stateChangelogTopics().iterator().asScala.map(_.name()):Iterator[String])
          ).distinct.toSeq

        // While correctness of the heartbeat request is checked inside the group coordinator,
        // we are checking early that topics in the topology have valid names and are not internal
        // kafka topics, since we need to pass it to the authorization helper before passing the
        // request to the group coordinator.

        val prohibitedTopics = requiredTopics.filter(Topic.isInternal)
        if (prohibitedTopics.nonEmpty) {
          val errorResponse = new StreamsGroupHeartbeatResponseData()
          errorResponse.setErrorCode(Errors.STREAMS_INVALID_TOPOLOGY.code)
          errorResponse.setErrorMessage(f"Use of Kafka internal topics ${prohibitedTopics.mkString(",")} in a Kafka Streams topology is prohibited.")
          requestHelper.sendMaybeThrottle(request, new StreamsGroupHeartbeatResponse(errorResponse))
          return CompletableFuture.completedFuture[Unit](())
        }

        val invalidTopics = requiredTopics.filterNot(Topic.isValid)
        if (invalidTopics.nonEmpty) {
          val errorResponse = new StreamsGroupHeartbeatResponseData()
          errorResponse.setErrorCode(Errors.STREAMS_INVALID_TOPOLOGY.code)
          errorResponse.setErrorMessage(f"Topic names ${invalidTopics.mkString(",")} are not valid topic names.")
          requestHelper.sendMaybeThrottle(request, new StreamsGroupHeartbeatResponse(errorResponse))
          return CompletableFuture.completedFuture[Unit](())
        }

        if (requiredTopics.nonEmpty) {
          val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC, requiredTopics)(identity)
          if (authorizedTopics.size < requiredTopics.size) {
            val responseData = new StreamsGroupHeartbeatResponseData().setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
            requestHelper.sendMaybeThrottle(request, new StreamsGroupHeartbeatResponse(responseData))
            return CompletableFuture.completedFuture[Unit](())
          }
        }
      }

      groupCoordinator.streamsGroupHeartbeat(
        request.context,
        streamsGroupHeartbeatRequest.data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, streamsGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          val responseData = response.data()
          val topicsToCreate = response.creatableTopics().asScala
          if (topicsToCreate.nonEmpty) {

            val createTopicUnauthorized =
              if(!authHelper.authorize(request.context, CREATE, CLUSTER, CLUSTER_NAME, logIfDenied = false))
                authHelper.partitionSeqByAuthorized(request.context, CREATE, TOPIC, topicsToCreate.keys.toSeq)(identity[String])._2
              else Set.empty

            if (createTopicUnauthorized.nonEmpty) {
              if (responseData.status() == null) {
                responseData.setStatus(new util.ArrayList());
              }
              val missingInternalTopicStatus =
                responseData.status().stream().filter(x => x.statusCode() == StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code()).findFirst()
              if (missingInternalTopicStatus.isPresent) {
                missingInternalTopicStatus.get().setStatusDetail(
                  missingInternalTopicStatus.get().statusDetail() + "; Unauthorized to CREATE on topics " + createTopicUnauthorized.mkString(", ") + "."
                )
              } else {
                responseData.status().add(
                  new StreamsGroupHeartbeatResponseData.Status()
                    .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
                    .setStatusDetail("Unauthorized to CREATE on topics " + createTopicUnauthorized.mkString(", ") + ".")
                )
              }
            } else {
              autoTopicCreationManager.createStreamsInternalTopics(topicsToCreate, requestContext);
            }
          }

          requestHelper.sendMaybeThrottle(request, new StreamsGroupHeartbeatResponse(responseData))
        }
      }
    }
  }

  def handleStreamsGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val streamsGroupDescribeRequest = request.body[StreamsGroupDescribeRequest]
    val includeAuthorizedOperations = streamsGroupDescribeRequest.data.includeAuthorizedOperations

    if (!isStreamsGroupProtocolEnabled) {
      // The API is not enabled by default. If it is not enabled, we fail directly here.
      requestHelper.sendMaybeThrottle(request, request.body[StreamsGroupDescribeRequest].getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new StreamsGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      streamsGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new StreamsGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.streamsGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, streamsGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (includeAuthorizedOperations) {
            results.forEach { groupResult =>
              if (groupResult.errorCode == Errors.NONE.code) {
                groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                  request,
                  new Resource(ResourceType.GROUP, groupResult.groupId)
                ))
              }
            }
          }

          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          // Clients are not allowed to see topics that are not authorized for Describe.
          if (authorizerPlugin.isDefined) {
            val topicsToCheck = response.groups.stream()
              .filter(group => group.topology != null)
              .flatMap(group => group.topology.subtopologies.stream)
              .flatMap(subtopology => java.util.stream.Stream.concat(
                java.util.stream.Stream.concat(
                  java.util.stream.Stream.concat(
                    subtopology.sourceTopics.stream,
                    subtopology.repartitionSinkTopics.stream),
                  subtopology.repartitionSourceTopics.stream.map(_.name)),
                subtopology.stateChangelogTopics.stream.map(_.name)))
              .collect(Collectors.toSet[String])
              .asScala

            val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
              topicsToCheck)(identity)

              val updatedGroups = response.groups.stream.map { group =>
                val hasUnauthorizedTopic = if (group.topology == null) false else
                  group.topology.subtopologies.stream()
                    .flatMap(subtopology => java.util.stream.Stream.concat(
                      java.util.stream.Stream.concat(
                        java.util.stream.Stream.concat(
                          subtopology.sourceTopics.stream,
                          subtopology.repartitionSinkTopics.stream),
                        subtopology.repartitionSourceTopics.stream.map(_.name)),
                      subtopology.stateChangelogTopics.stream.map(_.name)))
                    .anyMatch(topic => !authorizedTopics.contains(topic))

              if (hasUnauthorizedTopic) {
                new StreamsGroupDescribeResponseData.DescribedGroup()
                  .setGroupId(group.groupId)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setErrorMessage("The described group uses topics that the client is not authorized to describe.")
                  .setMembers(List.empty.asJava)
                  .setTopology(null)
              } else {
                group
              }
            }.collect(Collectors.toList[StreamsGroupDescribeResponseData.DescribedGroup])
            response.setGroups(updatedGroups)
          }

          requestHelper.sendMaybeThrottle(request, new StreamsGroupDescribeResponse(response))
        }
      }
    }

  }

  def handleGetTelemetrySubscriptionsRequest(request: RequestChannel.Request): Unit = {
    val subscriptionRequest = request.body[GetTelemetrySubscriptionsRequest]
    try {
      requestHelper.sendMaybeThrottle(request, clientMetricsManager.processGetTelemetrySubscriptionRequest(subscriptionRequest, request.context))
    } catch {
      case _: Exception =>
        requestHelper.sendMaybeThrottle(request, subscriptionRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
    }
  }

  private def handlePushTelemetryRequest(request: RequestChannel.Request): Unit = {
    val pushTelemetryRequest = request.body[PushTelemetryRequest]
    try {
      requestHelper.sendMaybeThrottle(request, clientMetricsManager.processPushTelemetryRequest(pushTelemetryRequest, request.context))
    } catch {
      case _: Exception =>
        requestHelper.sendMaybeThrottle(request, pushTelemetryRequest.getErrorResponse(Errors.INVALID_REQUEST.exception))
    }
  }

  /**
   * Handle ListConfigResourcesRequest. If resourceTypes are not specified, it uses ListConfigResourcesRequest#supportedResourceTypes
   * to retrieve config resources. If resourceTypes are specified, it returns matched config resources.
   * If a config resource type is not supported, the handler returns UNSUPPORTED_VERSION.
   */
  private def handleListConfigResources(request: RequestChannel.Request): Unit = {
    val listConfigResourcesRequest = request.body[ListConfigResourcesRequest]

    if (!authHelper.authorize(request.context, DESCRIBE_CONFIGS, CLUSTER, CLUSTER_NAME)) {
      requestHelper.sendMaybeThrottle(request, listConfigResourcesRequest.getErrorResponse(Errors.CLUSTER_AUTHORIZATION_FAILED.exception))
    } else {
      val data = new ListConfigResourcesResponseData()

      val supportedResourceTypes = listConfigResourcesRequest.supportedResourceTypes()
      var resourceTypes = listConfigResourcesRequest.data().resourceTypes()
      if (resourceTypes.isEmpty) {
        resourceTypes = supportedResourceTypes.stream().toList
      }

      resourceTypes.forEach(resourceType =>
        if (!supportedResourceTypes.contains(resourceType)) {
          requestHelper.sendMaybeThrottle(request, new ListConfigResourcesResponse(data.setErrorCode(Errors.UNSUPPORTED_VERSION.code())))
          return
        }
      )

      val result = new util.ArrayList[ListConfigResourcesResponseData.ConfigResource]()
      if (resourceTypes.contains(ConfigResource.Type.GROUP.id)) {
        groupConfigManager.groupIds().forEach(id =>
          result.add(new ListConfigResourcesResponseData.ConfigResource().setResourceName(id).setResourceType(ConfigResource.Type.GROUP.id))
        )
      }
      if (resourceTypes.contains(ConfigResource.Type.CLIENT_METRICS.id)) {
        clientMetricsManager.listClientMetricsResources.forEach(name =>
          result.add(new ListConfigResourcesResponseData.ConfigResource().setResourceName(name).setResourceType(ConfigResource.Type.CLIENT_METRICS.id))
        )
      }
      if (resourceTypes.contains(ConfigResource.Type.BROKER_LOGGER.id)) {
        metadataCache.getBrokerNodes(request.context.listenerName).forEach(node =>
          result.add(new ListConfigResourcesResponseData.ConfigResource().setResourceName(node.id.toString).setResourceType(ConfigResource.Type.BROKER_LOGGER.id))
        )
      }
      if (resourceTypes.contains(ConfigResource.Type.BROKER.id)) {
        metadataCache.getBrokerNodes(request.context.listenerName).forEach(node =>
          result.add(new ListConfigResourcesResponseData.ConfigResource().setResourceName(node.id.toString).setResourceType(ConfigResource.Type.BROKER.id))
        )
      }
      if (resourceTypes.contains(ConfigResource.Type.TOPIC.id)) {
        metadataCache.getAllTopics.forEach(name =>
          result.add(new ListConfigResourcesResponseData.ConfigResource().setResourceName(name).setResourceType(ConfigResource.Type.TOPIC.id))
        )
      }
      data.setConfigResources(result)
      requestHelper.sendMaybeThrottle(request, new ListConfigResourcesResponse(data))
    }
  }

  def handleShareGroupHeartbeat(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareGroupHeartbeatRequest = request.body[ShareGroupHeartbeatRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, shareGroupHeartbeatRequest.data.groupId)) {
      requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      if (shareGroupHeartbeatRequest.data.subscribedTopicNames != null &&
        !shareGroupHeartbeatRequest.data.subscribedTopicNames.isEmpty) {
        // Check the authorization if the subscribed topic names are provided.
        // Clients are not allowed to see topics that are not authorized for Describe.
        val subscribedTopicSet = shareGroupHeartbeatRequest.data.subscribedTopicNames.asScala.toSet
        val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
          subscribedTopicSet)(identity)
        if (authorizedTopics.size < subscribedTopicSet.size) {
          val responseData = new ShareGroupHeartbeatResponseData()
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          requestHelper.sendMaybeThrottle(request, new ShareGroupHeartbeatResponse(responseData))
          return CompletableFuture.completedFuture[Unit](())
        }
      }

      groupCoordinator.shareGroupHeartbeat(
        request.context,
        shareGroupHeartbeatRequest.data
      ).handle[Unit] { (response, exception) =>

        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareGroupHeartbeatRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ShareGroupHeartbeatResponse(response))
        }
      }
    }
  }

  def handleShareGroupDescribe(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareGroupDescribeRequest = request.body[ShareGroupDescribeRequest]
    val includeAuthorizedOperations = shareGroupDescribeRequest.data.includeAuthorizedOperations

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareGroupDescribeRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      CompletableFuture.completedFuture[Unit](())
    } else {
      val response = new ShareGroupDescribeResponseData()

      val authorizedGroups = new ArrayBuffer[String]()
      shareGroupDescribeRequest.data.groupIds.forEach { groupId =>
        if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupId)) {
          response.groups.add(new ShareGroupDescribeResponseData.DescribedGroup()
            .setGroupId(groupId)
            .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)
          )
        } else {
          authorizedGroups += groupId
        }
      }

      groupCoordinator.shareGroupDescribe(
        request.context,
        authorizedGroups.asJava
      ).handle[Unit] { (results, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareGroupDescribeRequest.getErrorResponse(exception))
        } else {
          if (includeAuthorizedOperations) {
            results.forEach { groupResult =>
              if (groupResult.errorCode == Errors.NONE.code) {
                groupResult.setAuthorizedOperations(authHelper.authorizedOperations(
                  request,
                  new Resource(ResourceType.GROUP, groupResult.groupId)
                ))
              }
            }
          }

          if (response.groups.isEmpty) {
            // If the response is empty, we can directly reuse the results.
            response.setGroups(results)
          } else {
            // Otherwise, we have to copy the results into the existing ones.
            response.groups.addAll(results)
          }

          // Clients are not allowed to see topics that are not authorized for Describe.
          if (authorizerPlugin.isDefined) {
            val topicsToCheck = response.groups.stream()
              .flatMap(group => group.members.stream)
              .flatMap(member => member.assignment.topicPartitions.stream)
              .map(topicPartition => topicPartition.topicName)
              .collect(Collectors.toSet[String])
              .asScala
            val authorizedTopics = authHelper.filterByAuthorized(request.context, DESCRIBE, TOPIC,
              topicsToCheck)(identity)
            val updatedGroups = response.groups.stream().map { group =>
              val hasUnauthorizedTopic = group.members.stream()
                .flatMap(member => member.assignment.topicPartitions.stream)
                .anyMatch(tp => !authorizedTopics.contains(tp.topicName))

              if (hasUnauthorizedTopic) {
                new ShareGroupDescribeResponseData.DescribedGroup()
                  .setGroupId(group.groupId)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setErrorMessage("The group has described topic(s) that the client is not authorized to describe.")
                  .setMembers(List.empty.asJava)
              } else {
                group
              }
            }.collect(Collectors.toList[ShareGroupDescribeResponseData.DescribedGroup])
            response.setGroups(updatedGroups)
          }

          requestHelper.sendMaybeThrottle(request, new ShareGroupDescribeResponse(response))
        }
      }
    }
  }

  /**
   * Handle a shareFetch request
   */
  def handleShareFetchRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareFetchRequest = request.body[ShareFetchRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

    val groupId = shareFetchRequest.data.groupId

    // Share Fetch needs permission to perform the READ action on the named group resource (groupId)
    if (!authHelper.authorize(request.context, READ, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

    val memberId = shareFetchRequest.data.memberId
    val shareSessionEpoch = shareFetchRequest.data.shareSessionEpoch

    def isAcknowledgeDataPresentInFetchRequest: Boolean = {
      shareFetchRequest.data.topics.asScala
        .flatMap(t => t.partitions().asScala)
        .exists(partition => partition.acknowledgementBatches != null && !partition.acknowledgementBatches.isEmpty)
    }

    val isAcknowledgeDataPresent = isAcknowledgeDataPresentInFetchRequest
    val topicIdNames = metadataCache.topicIdsToNames()

    val shareFetchData = shareFetchRequest.shareFetchData(topicIdNames)
    val forgottenTopics = shareFetchRequest.forgottenTopics(topicIdNames)

    val newReqMetadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.fromString(memberId), shareSessionEpoch)
    var shareFetchContext: ShareFetchContext = null

    try {
      // Creating the shareFetchContext for Share Session Handling. if context creation fails, the request is failed directly here.
      shareFetchContext = sharePartitionManager.newContext(groupId, shareFetchData, forgottenTopics, newReqMetadata, isAcknowledgeDataPresent, request.context.connectionId)
    } catch {
      case _: ShareSessionLimitReachedException =>
        sharePartitionManager.createIdleShareFetchTimerTask(shareFetchRequest.maxWait).handle(
          (_, exception) => {
            if (exception != null) {
              requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, exception))
            } else {
              requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.SHARE_SESSION_LIMIT_REACHED.exception))
            }
          }
        )
        return CompletableFuture.completedFuture[Unit](())
      case e: Exception =>
        requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e))
        return CompletableFuture.completedFuture[Unit](())
    }

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData = shareFetchContext.getErroneousAndValidTopicIdPartitions
    val topicIdPartitionSeq: mutable.Set[TopicIdPartition] = mutable.Set()
    erroneousAndValidPartitionData.erroneous.forEach {
      case(tp, _) => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp
    }
    erroneousAndValidPartitionData.validTopicIdPartitions.forEach(tp => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp)
    shareFetchData.forEach { tp => if (!topicIdPartitionSeq.contains(tp)) topicIdPartitionSeq += tp}

    // Kafka share consumers need READ permission on each topic they are fetching.
    val authorizedTopics = authHelper.filterByAuthorized(
      request.context,
      READ,
      TOPIC,
      topicIdPartitionSeq
    )(_.topicPartition.topic)

    // Variable to store the topic partition wise result of piggybacked acknowledgements.
    var acknowledgeResult: CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]] =
      CompletableFuture.completedFuture(mutable.Map.empty)

    // Handling the Acknowledgements from the ShareFetchRequest If this check is true, we are sure that this is not an
    // Initial ShareFetch Request, otherwise the request would have been invalid.
    if (isAcknowledgeDataPresent) {
      val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()
      val acknowledgementDataFromRequest = getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicIdNames, erroneous)
      acknowledgeResult = handleAcknowledgements(
        acknowledgementDataFromRequest,
        erroneous,
        sharePartitionManager,
        authorizedTopics,
        groupId,
        memberId,
      )
    }

    // Handling the Fetch from the ShareFetchRequest.
    // Variable to store the topic partition wise result of fetching.
    val fetchResult: CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]] = handleFetchFromShareFetchRequest(
      request,
      shareSessionEpoch,
      erroneousAndValidPartitionData,
      sharePartitionManager,
      authorizedTopics
    )

    def combineShareFetchAndShareAcknowledgeResponses(fetchResult: CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]],
                                                      acknowledgeResult: CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]],
                                                     ): CompletableFuture[ShareFetchResponse] = {

      fetchResult.thenCombine(acknowledgeResult,
        (fetchMap: Map[TopicIdPartition, ShareFetchResponseData.PartitionData],
          acknowledgeMap: Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]) => {
          val shareFetchResponse = processShareFetchResponse(fetchMap, request, topicIdNames, shareFetchContext)
          // The outer map has topicId as the key and the inner map has partitionIndex as the key.
          val topicPartitionAcknowledgements: mutable.Map[Uuid, mutable.Map[Int, Short]] = mutable.Map()
          if (acknowledgeMap != null && acknowledgeMap.nonEmpty) {
            acknowledgeMap.foreach { case (tp, partitionData) =>
              topicPartitionAcknowledgements.get(tp.topicId) match {
                case Some(subMap) =>
                  subMap += tp.partition -> partitionData.errorCode
                case None =>
                  val partitionAcknowledgementsMap: mutable.Map[Int, Short] = mutable.Map()
                  partitionAcknowledgementsMap += tp.partition -> partitionData.errorCode
                  topicPartitionAcknowledgements += tp.topicId -> partitionAcknowledgementsMap
              }
            }
          }

          shareFetchResponse.data.responses.forEach{ topic =>
            val topicId = topic.topicId
            topicPartitionAcknowledgements.get(topicId) match {
              case Some(subMap) =>
                topic.partitions.forEach { partition =>
                  subMap.get(partition.partitionIndex) match {
                    case Some(value) =>
                      partition.setAcknowledgeErrorCode(value)
                      // Delete the element.
                      subMap.remove(partition.partitionIndex)
                    case None =>
                  }
                }
                // Add the remaining acknowledgements.
                subMap.foreach { case (partitionIndex, value) =>
                  val fetchPartitionData = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(partitionIndex)
                    .setErrorCode(Errors.NONE.code)
                    .setAcknowledgeErrorCode(value)
                    .setRecords(MemoryRecords.EMPTY)
                  topic.partitions.add(fetchPartitionData)
                }
                topicPartitionAcknowledgements.remove(topicId)
              case None =>
            }
          }
          // Add the remaining acknowledgements.
          topicPartitionAcknowledgements.foreach { case (topicId, subMap) =>
            val topicData = new ShareFetchResponseData.ShareFetchableTopicResponse()
              .setTopicId(topicId)
            subMap.foreach { case (partitionIndex, value) =>
              val fetchPartitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(Errors.NONE.code)
                .setAcknowledgeErrorCode(value)
                .setRecords(MemoryRecords.EMPTY)
              topicData.partitions.add(fetchPartitionData)
            }
            shareFetchResponse.data.responses.add(topicData)
          }

          if (shareSessionEpoch == ShareRequestMetadata.FINAL_EPOCH) {
            sharePartitionManager.releaseSession(groupId, memberId).
              whenComplete((releaseAcquiredRecordsData, throwable) =>
                if (throwable != null) {
                  error(s"Releasing share session close with correlation from client ${request.header.clientId}  " +
                    s"failed with error ${throwable.getMessage}")
                } else {
                  info(s"Releasing share session close $releaseAcquiredRecordsData succeeded")
                }
              )
          }
          shareFetchResponse
        })
    }

    // Send the response once the future completes.
    combineShareFetchAndShareAcknowledgeResponses(fetchResult, acknowledgeResult).handle[Unit] {(result, exception) =>
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, shareFetchRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, exception))
      } else {
        requestChannel.sendResponse(request, result, None)
      }
    }
  }

  // Visible for Testing
  def handleFetchFromShareFetchRequest(request: RequestChannel.Request,
                                       shareSessionEpoch: Int,
                                       erroneousAndValidPartitionData: ErroneousAndValidPartitionData,
                                       sharePartitionManagerInstance: SharePartitionManager,
                                       authorizedTopics: Set[String]
                                      ): CompletableFuture[Map[TopicIdPartition, ShareFetchResponseData.PartitionData]] = {

    val erroneous = mutable.Map.empty[TopicIdPartition, ShareFetchResponseData.PartitionData]
    erroneousAndValidPartitionData.erroneous.forEach { (topicIdPartition, partitionData) => erroneous.put(topicIdPartition, partitionData) }

    val interestedTopicPartitions = new util.ArrayList[TopicIdPartition]

    erroneousAndValidPartitionData.validTopicIdPartitions.forEach { topicIdPartition =>
      if (!authorizedTopics.contains(topicIdPartition.topicPartition.topic))
        erroneous += topicIdPartition -> ShareFetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicIdPartition.topicPartition))
        erroneous += topicIdPartition -> ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        interestedTopicPartitions.add(topicIdPartition)
    }

    val shareFetchRequest = request.body[ShareFetchRequest]

    val clientId = request.header.clientId
    val groupId = shareFetchRequest.data.groupId

    if (interestedTopicPartitions.isEmpty) {
      CompletableFuture.completedFuture(erroneous)
    } else {
      // for share fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being
      // throttled given no bytes were recorded in the recent quota window. Trying to fetch more bytes would result
      // in a guaranteed throttling potentially blocking consumer progress.
      val maxQuotaWindowBytes = quotas.fetch.maxValueInQuotaWindow(request.session, clientId).toInt

      val fetchMaxBytes = Math.min(Math.min(shareFetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(shareFetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] =
        Optional.of(new DefaultClientMetadata(
          CommonClientConfigs.DEFAULT_CLIENT_RACK,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))

      val params = new FetchParams(
        FetchRequest.CONSUMER_REPLICA_ID,
        -1,
        shareFetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.of(FetchRequest.CONSUMER_REPLICA_ID, groupConfigManager.groupConfig(groupId).map(_.shareIsolationLevel()).orElse(GroupConfig.defaultShareIsolationLevel)),
        clientMetadata,
        true
      )

      // call the share partition manager to fetch messages from the local replica.
      sharePartitionManagerInstance.fetchMessages(
        groupId,
        shareFetchRequest.data.memberId,
        params,
        shareSessionEpoch,
        shareFetchRequest.data.maxRecords,
        shareFetchRequest.data.batchSize,
        interestedTopicPartitions
      ).thenApply{ result =>
        val combinedResult = mutable.Map.empty[TopicIdPartition, ShareFetchResponseData.PartitionData]
        result.asScala.foreach { case (tp, data) =>
          combinedResult += (tp -> data)
        }
        erroneous.foreach { case (tp, data) =>
          combinedResult += (tp -> data)
        }
        combinedResult.toMap
      }
    }
  }

  // Visible for Testing
  def handleAcknowledgements(acknowledgementData: mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]],
                             erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData],
                             sharePartitionManagerInstance: SharePartitionManager,
                             authorizedTopics: Set[String],
                             groupId: String,
                             memberId: String): CompletableFuture[Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]] = {

    val erroneousTopicIdPartitions = validateAcknowledgementBatches(acknowledgementData, erroneous)
    erroneousTopicIdPartitions.foreach(tp => acknowledgementData.remove(tp))

    val interested = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    val emptyAcknowledgements = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    acknowledgementData.foreach{
      case (topicIdPartition: TopicIdPartition, acknowledgeBatches: util.List[ShareAcknowledgementBatch]) =>
        if (!authorizedTopics.contains(topicIdPartition.topicPartition.topic))
          erroneous += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else if (acknowledgeBatches.size() == 0)
          emptyAcknowledgements += topicIdPartition ->
            ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.NONE)
        else {
          interested += topicIdPartition -> acknowledgeBatches
        }
    }

    if (interested.isEmpty) {
      CompletableFuture.completedFuture(erroneous)
    } else {
      // call the share partition manager to acknowledge messages in the share partition
      sharePartitionManagerInstance.acknowledge(
        memberId,
        groupId,
        interested.asJava
      ).thenApply{ result =>
        val combinedResult = mutable.Map.empty[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
        result.asScala.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        erroneous.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        emptyAcknowledgements.foreach{ case (tp, data) =>
          combinedResult += (tp -> data)
        }
        combinedResult.toMap
      }
    }
  }

  def handleShareAcknowledgeRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val shareAcknowledgeRequest = request.body[ShareAcknowledgeRequest]

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request,
        shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

    val groupId = shareAcknowledgeRequest.data.groupId

    // Share Acknowledge needs permission to perform READ action on the named group resource (groupId)
    if (!authHelper.authorize(request.context, READ, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request,
        shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

    val memberId = shareAcknowledgeRequest.data.memberId
    val shareSessionEpoch = shareAcknowledgeRequest.data.shareSessionEpoch
    val newReqMetadata: ShareRequestMetadata = new ShareRequestMetadata(Uuid.fromString(memberId), shareSessionEpoch)

    try {
      // Updating the cache for Share Session Handling
      sharePartitionManager.acknowledgeSessionUpdate(groupId, newReqMetadata)
    } catch {
      case e: Exception =>
        requestHelper.sendMaybeThrottle(request, shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e))
        return CompletableFuture.completedFuture[Unit](())
    }

    val topicIdPartitionSeq: mutable.Set[TopicIdPartition] = mutable.Set()
    val shareAcknowledgeData = shareAcknowledgeRequest.data

    val topicIdNames = metadataCache.topicIdsToNames()

    shareAcknowledgeData.topics.forEach{ topic =>
      topic.partitions.forEach{ partition =>
        val topicIdPartition = new TopicIdPartition(topic.topicId,
          new TopicPartition(topicIdNames.get(topic.topicId), partition.partitionIndex))
        topicIdPartitionSeq += topicIdPartition
      }
    }

    val authorizedTopics = authHelper.filterByAuthorized(
      request.context,
      READ,
      TOPIC,
      topicIdPartitionSeq
    )(_.topicPartition.topic)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()
    val acknowledgementDataFromRequest = getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicIdNames, erroneous)
    handleAcknowledgements(acknowledgementDataFromRequest, erroneous, sharePartitionManager, authorizedTopics, groupId, memberId)
      .handle[Unit] {(result, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, shareAcknowledgeRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, exception))
        } else {
          if (shareSessionEpoch == ShareRequestMetadata.FINAL_EPOCH) {
            sharePartitionManager.releaseSession(groupId, memberId).
              whenComplete{ (releaseAcquiredRecordsData, throwable) =>
                if (throwable != null) {
                  debug(s"Releasing share session close with correlation from client ${request.header.clientId}  " +
                    s"failed with error ${throwable.getMessage}")
                } else {
                  info(s"Releasing share session close $releaseAcquiredRecordsData succeeded")
                }
              }
          }
          requestHelper.sendMaybeThrottle(request, processShareAcknowledgeResponse(result, request))
        }
      }
  }

  def handleInitializeShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val initializeShareGroupStateRequest = request.body[InitializeShareGroupStateRequest]
    // We do not need a check for isShareGroupProtocolEnabled in this RPC since there is a check for it in ShareFetch/ShareAcknowledge RPCs,
    // hence requests won't reach Persister.

    if (!authorizeClusterOperation(request, CLUSTER_ACTION)) {
      requestHelper.sendMaybeThrottle(request, new InitializeShareGroupStateResponse(
        InitializeShareGroupStateResponse.toGlobalErrorResponse(
          initializeShareGroupStateRequest.data(),
          Errors.CLUSTER_AUTHORIZATION_FAILED
        )))
      return CompletableFuture.completedFuture[Unit](())
    }

    shareCoordinator.initializeState(request.context, initializeShareGroupStateRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, initializeShareGroupStateRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new InitializeShareGroupStateResponse(response))
        }
      }
  }

  def handleReadShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val readShareGroupStateRequest = request.body[ReadShareGroupStateRequest]
    // We do not need a check for isShareGroupProtocolEnabled in this RPC since there is a check for it in ShareFetch/ShareAcknowledge RPCs,
    // hence requests won't reach Persister.

    if (!authorizeClusterOperation(request, CLUSTER_ACTION)) {
      requestHelper.sendMaybeThrottle(request, new ReadShareGroupStateResponse(
        ReadShareGroupStateResponse.toGlobalErrorResponse(
          readShareGroupStateRequest.data(),
          Errors.CLUSTER_AUTHORIZATION_FAILED
        )))
      return CompletableFuture.completedFuture[Unit](())
    }

    shareCoordinator.readState(request.context, readShareGroupStateRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, readShareGroupStateRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ReadShareGroupStateResponse(response))
        }
      }
  }

  def handleWriteShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val writeShareGroupStateRequest = request.body[WriteShareGroupStateRequest]
    // We do not need a check for isShareGroupProtocolEnabled in this RPC since there is a check for it in ShareFetch/ShareAcknowledge RPCs,
    // hence requests won't reach Persister.

    if (!authorizeClusterOperation(request, CLUSTER_ACTION)) {
      requestHelper.sendMaybeThrottle(request, new WriteShareGroupStateResponse(
        WriteShareGroupStateResponse.toGlobalErrorResponse(
          writeShareGroupStateRequest.data(),
          Errors.CLUSTER_AUTHORIZATION_FAILED
        )))
      return CompletableFuture.completedFuture[Unit](())
    }

    shareCoordinator.writeState(request.context, writeShareGroupStateRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, writeShareGroupStateRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new WriteShareGroupStateResponse(response))
        }
      }
  }

  def handleDeleteShareGroupStateRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteShareGroupStateRequest = request.body[DeleteShareGroupStateRequest]
    // We do not need a check for isShareGroupProtocolEnabled in this RPC since there is a check for it in ShareFetch/ShareAcknowledge RPCs,
    // hence requests won't reach Persister.

    if (!authorizeClusterOperation(request, CLUSTER_ACTION)) {
      requestHelper.sendMaybeThrottle(request, new DeleteShareGroupStateResponse(
        DeleteShareGroupStateResponse.toGlobalErrorResponse(
          deleteShareGroupStateRequest.data(),
          Errors.CLUSTER_AUTHORIZATION_FAILED
        )))
      return CompletableFuture.completedFuture[Unit](())
    }

    shareCoordinator.deleteState(request.context, deleteShareGroupStateRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, deleteShareGroupStateRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new DeleteShareGroupStateResponse(response))
        }
      }
  }

  def handleReadShareGroupStateSummaryRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val readShareGroupStateSummaryRequest = request.body[ReadShareGroupStateSummaryRequest]
    // We do not need a check for isShareGroupProtocolEnabled in this RPC since there is a check for it in ShareFetch/ShareAcknowledge RPCs,
    // hence requests won't reach Persister.

    if (!authorizeClusterOperation(request, CLUSTER_ACTION)) {
      requestHelper.sendMaybeThrottle(request, new ReadShareGroupStateSummaryResponse(
        ReadShareGroupStateSummaryResponse.toGlobalErrorResponse(
          readShareGroupStateSummaryRequest.data(),
          Errors.CLUSTER_AUTHORIZATION_FAILED
        )))
      return CompletableFuture.completedFuture[Unit](())
    }

    shareCoordinator.readStateSummary(request.context, readShareGroupStateSummaryRequest.data)
      .handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, readShareGroupStateSummaryRequest.getErrorResponse(exception))
        } else {
          requestHelper.sendMaybeThrottle(request, new ReadShareGroupStateSummaryResponse(response))
        }
      }
  }

  def handleDescribeShareGroupOffsetsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val describeShareGroupOffsetsRequest = request.body[DescribeShareGroupOffsetsRequest]
    val groups = describeShareGroupOffsetsRequest.groups()

    val futures = new mutable.ArrayBuffer[CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]](groups.size)
    groups.forEach { groupDescribeOffsets =>
      val isAllPartitions = groupDescribeOffsets.topics == null
      if (!isShareGroupProtocolEnabled) {
        futures += CompletableFuture.completedFuture(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
          .setGroupId(groupDescribeOffsets.groupId)
          .setErrorCode(Errors.UNSUPPORTED_VERSION.code))
      } else if (!authHelper.authorize(request.context, DESCRIBE, GROUP, groupDescribeOffsets.groupId)) {
        futures += CompletableFuture.completedFuture(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
          .setGroupId(groupDescribeOffsets.groupId)
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code))
      } else if (isAllPartitions) {
        futures += describeShareGroupAllOffsetsForGroup(
          request.context,
          groupDescribeOffsets
        )
      } else if (groupDescribeOffsets.topics.isEmpty) {
        futures += CompletableFuture.completedFuture(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
          .setGroupId(groupDescribeOffsets.groupId))
      } else {
        futures += describeShareGroupOffsetsForGroup(
          request.context,
          groupDescribeOffsets
        )
      }
    }

    CompletableFuture.allOf(futures.toArray: _*).handle[Unit] { (_, _) =>
      val groupResponses = new ArrayBuffer[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup](futures.size)
      val responseData = new DescribeShareGroupOffsetsResponseData().setGroups(groupResponses.asJava)
      futures.foreach(future => groupResponses += future.join)
      requestHelper.sendMaybeThrottle(request, new DescribeShareGroupOffsetsResponse(responseData))
    }
  }

  private def describeShareGroupAllOffsetsForGroup(requestContext: RequestContext,
    groupDescribeOffsetsRequest: DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup
  ): CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup] = {
    groupCoordinator.describeShareGroupAllOffsets(
      requestContext,
      groupDescribeOffsetsRequest
    ).handle[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup] { (groupDescribeOffsetsResponse, exception) =>
      if (exception != null) {
        val error = Errors.forException(exception)
        new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
          .setGroupId(groupDescribeOffsetsRequest.groupId)
          .setErrorCode(error.code)
          .setErrorMessage(error.message)
      } else {
        // Clients are not allowed to see offsets for topics that are not authorized for Describe.
        val (authorizedOffsets, _) = authHelper.partitionSeqByAuthorized(
          requestContext,
          DESCRIBE,
          TOPIC,
          groupDescribeOffsetsResponse.topics.asScala
        )(_.topicName)
        groupDescribeOffsetsResponse.setTopics(authorizedOffsets.asJava)
      }
    }
  }

  private def describeShareGroupOffsetsForGroup(requestContext: RequestContext,
    groupDescribeOffsetsRequest: DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup
  ): CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup] = {
    // Clients are not allowed to see offsets for topics that are not authorized for Describe.
    val (authorizedTopics, unauthorizedTopics) = authHelper.partitionSeqByAuthorized(
      requestContext,
      DESCRIBE,
      TOPIC,
      groupDescribeOffsetsRequest.topics.asScala
    )(_.topicName)

    groupCoordinator.describeShareGroupOffsets(
      requestContext,
      new DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup()
        .setGroupId(groupDescribeOffsetsRequest.groupId)
        .setTopics(authorizedTopics.asJava)
    ).handle[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup] { (groupDescribeOffsetsResponse, exception) =>
      if (exception != null) {
        val error = Errors.forException(exception)
        new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup()
          .setGroupId(groupDescribeOffsetsRequest.groupId)
          .setErrorCode(error.code)
          .setErrorMessage(error.message)
      } else if (groupDescribeOffsetsResponse.errorCode() != Errors.NONE.code) {
        groupDescribeOffsetsResponse
      } else {
        val topics = new util.ArrayList[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic](
          groupDescribeOffsetsResponse.topics.size + unauthorizedTopics.size
        )
        topics.addAll(groupDescribeOffsetsResponse.topics)
        unauthorizedTopics.foreach { topic =>
          val topicResponse = new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseTopic()
            .setTopicName(topic.topicName)
            .setTopicId(Uuid.ZERO_UUID)
          topic.partitions().forEach { partitionIndex =>
            topicResponse.partitions.add(new DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(partitionIndex)
              .setStartOffset(-1)
              .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
              .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message))
          }
          topics.add(topicResponse)
        }
        groupDescribeOffsetsResponse.setTopics(topics)
      }
    }
  }

  def handleAlterShareGroupOffsetsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val alterShareGroupOffsetsRequest = request.body[AlterShareGroupOffsetsRequest]
    val groupId = alterShareGroupOffsetsRequest.data.groupId

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, alterShareGroupOffsetsRequest.getErrorResponse(Errors.UNSUPPORTED_VERSION.exception))
      return CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, READ, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request, alterShareGroupOffsetsRequest.getErrorResponse(Errors.GROUP_AUTHORIZATION_FAILED.exception))
    } else {
      val responseBuilder = new AlterShareGroupOffsetsResponse.Builder()
      val authorizedTopicPartitions = new AlterShareGroupOffsetsRequestData.AlterShareGroupOffsetsRequestTopicCollection()

      alterShareGroupOffsetsRequest.data.topics.forEach(topic => {
        val topicError = {
          if (!authHelper.authorize(request.context, READ, TOPIC, topic.topicName)) {
            Some(new ApiError(Errors.TOPIC_AUTHORIZATION_FAILED))
          } else if (!metadataCache.contains(topic.topicName)) {
            Some(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION))
          } else {
            None
          }
        }
        topicError match {
          case Some(error) =>
            topic.partitions.forEach(partition => responseBuilder.addPartition(topic.topicName, partition.partitionIndex, metadataCache.topicNamesToIds, error))
          case None =>
            authorizedTopicPartitions.add(topic.duplicate)
        }
      })

      val data = new AlterShareGroupOffsetsRequestData()
        .setGroupId(groupId)
        .setTopics(authorizedTopicPartitions)
      groupCoordinator.alterShareGroupOffsets(
        request.context,
        groupId,
        data
      ).handle[Unit] { (response, exception) =>
        if (exception != null) {
          requestHelper.sendMaybeThrottle(request, alterShareGroupOffsetsRequest.getErrorResponse(exception))
        } else if (response.errorCode != Errors.NONE.code) {
          requestHelper.sendMaybeThrottle(request, alterShareGroupOffsetsRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, response.errorCode, response.errorMessage))
        } else {
          requestHelper.sendMaybeThrottle(request, responseBuilder.merge(response, metadataCache.topicNamesToIds).build())
        }
      }
    }
    CompletableFuture.completedFuture[Unit](())
  }

  def handleDeleteShareGroupOffsetsRequest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteShareGroupOffsetsRequest = request.body[DeleteShareGroupOffsetsRequest]

    val groupId = deleteShareGroupOffsetsRequest.data.groupId

    if (!isShareGroupProtocolEnabled) {
      requestHelper.sendMaybeThrottle(request, deleteShareGroupOffsetsRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.UNSUPPORTED_VERSION.exception))
      return CompletableFuture.completedFuture[Unit](())
    } else if (!authHelper.authorize(request.context, DELETE, GROUP, groupId)) {
      requestHelper.sendMaybeThrottle(request, deleteShareGroupOffsetsRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, Errors.GROUP_AUTHORIZATION_FAILED.exception))
      return CompletableFuture.completedFuture[Unit](())
    }

    val deleteShareGroupOffsetsResponseTopics: util.List[DeleteShareGroupOffsetsResponseTopic] = new util.ArrayList[DeleteShareGroupOffsetsResponseTopic]()

    val authorizedTopics: util.List[DeleteShareGroupOffsetsRequestTopic] =
      new util.ArrayList[DeleteShareGroupOffsetsRequestTopic]

    deleteShareGroupOffsetsRequest.data.topics.forEach{ topic =>
      if (!authHelper.authorize(request.context, READ, TOPIC, topic.topicName)) {
        deleteShareGroupOffsetsResponseTopics.add(
          new DeleteShareGroupOffsetsResponseData.DeleteShareGroupOffsetsResponseTopic()
            .setTopicName(topic.topicName)
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
            .setErrorMessage(Errors.TOPIC_AUTHORIZATION_FAILED.message)
        )
      } else {
        authorizedTopics.add(topic)
      }
    }

    groupCoordinator.deleteShareGroupOffsets(
      request.context,
      new DeleteShareGroupOffsetsRequestData().setGroupId(groupId).setTopics(authorizedTopics)
    ).handle[Unit] {(responseData, exception) => {
      if (exception != null) {
        requestHelper.sendMaybeThrottle(request, deleteShareGroupOffsetsRequest.getErrorResponse(
          AbstractResponse.DEFAULT_THROTTLE_TIME,
          Errors.forException(exception).code,
          exception.getMessage))
      } else if (responseData.errorCode() != Errors.NONE.code) {
        requestHelper.sendMaybeThrottle(
          request,
          deleteShareGroupOffsetsRequest.getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, responseData.errorCode, responseData.errorMessage)
        )
      } else {
        responseData.responses.forEach { topic => {
          deleteShareGroupOffsetsResponseTopics.add(topic)
        }}
        val deleteShareGroupStateResponse = new DeleteShareGroupOffsetsResponse(new DeleteShareGroupOffsetsResponseData()
          .setResponses(deleteShareGroupOffsetsResponseTopics))
        requestHelper.sendMaybeThrottle(request, deleteShareGroupStateResponse)
      }
    }}
  }

  // Visible for Testing
  def getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest: ShareAcknowledgeRequest,
                                                       topicIdNames: util.Map[Uuid, String],
                                                       erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                                      ): mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]] = {
    val acknowledgeBatchesMap = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    shareAcknowledgeRequest.data().topics().forEach{ topic =>
      if (!topicIdNames.containsKey(topic.topicId)) {
        topic.partitions.forEach{ case partition: ShareAcknowledgeRequestData.AcknowledgePartition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId,
            new TopicPartition(null, partition.partitionIndex))
          erroneous +=
            topicIdPartition -> ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        }
      } else {
        topic.partitions().forEach{ partition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId(),
            new TopicPartition(topicIdNames.get(topic.topicId()), partition.partitionIndex())
          )
          val acknowledgeBatches = new util.ArrayList[ShareAcknowledgementBatch]()
          partition.acknowledgementBatches().forEach{ batch =>
            acknowledgeBatches.add(new ShareAcknowledgementBatch(
              batch.firstOffset(),
              batch.lastOffset(),
              batch.acknowledgeTypes()
            ))
          }
          acknowledgeBatchesMap += topicIdPartition -> acknowledgeBatches
        }
      }
    }
    acknowledgeBatchesMap
  }

  // Visible for Testing
  def getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest: ShareFetchRequest,
                                                 topicIdNames: util.Map[Uuid, String],
                                                 erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                                ): mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]] = {

    val acknowledgeBatchesMap = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    shareFetchRequest.data().topics().forEach{ topic =>
      if (!topicIdNames.containsKey(topic.topicId)) {
        topic.partitions.forEach{ partition: ShareFetchRequestData.FetchPartition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId,
            new TopicPartition(null, partition.partitionIndex))
          erroneous +=
            topicIdPartition -> ShareAcknowledgeResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        }
      } else {
        topic.partitions().forEach { partition =>
          val topicIdPartition = new TopicIdPartition(
            topic.topicId(),
            new TopicPartition(topicIdNames.get(topic.topicId()), partition.partitionIndex())
          )
          val acknowledgeBatches = new util.ArrayList[ShareAcknowledgementBatch]()
          partition.acknowledgementBatches().forEach{ batch =>
            acknowledgeBatches.add(new ShareAcknowledgementBatch(
              batch.firstOffset(),
              batch.lastOffset(),
              batch.acknowledgeTypes()
            ))
          }
          acknowledgeBatchesMap += topicIdPartition -> acknowledgeBatches
        }
      }
    }
    acknowledgeBatchesMap
  }

  // the callback for processing a share acknowledge response, invoked before throttling
  def processShareAcknowledgeResponse(responseAcknowledgeData: Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData],
                                      request: RequestChannel.Request): ShareAcknowledgeResponse = {
    val partitions = new util.LinkedHashMap[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
    val nodeEndpoints = new mutable.HashMap[Int, Node]
    responseAcknowledgeData.foreach{ case(tp, partitionData) =>
      partitionData.errorCode() match {
        case errCode if errCode == Errors.NOT_LEADER_OR_FOLLOWER.code | errCode == Errors.FENCED_LEADER_EPOCH.code =>
          val leaderNode = getCurrentLeader(tp.topicPartition(), request.context.listenerName)
          leaderNode.node.foreach { node =>
            nodeEndpoints.put(node.id(), node)
          }
          partitionData.currentLeader()
            .setLeaderId(leaderNode.leaderId)
            .setLeaderEpoch(leaderNode.leaderEpoch)
        case _ =>
      }
      partitions.put(tp, partitionData)
    }

    ShareAcknowledgeResponse.of(
      Errors.NONE,
      0,
      partitions,
      nodeEndpoints.values.toList.asJava
    )
  }

  // Visible for Testing
  def validateAcknowledgementBatches(acknowledgementDataFromRequest: mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]],
                                     erroneous: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]
                                    ): mutable.Set[TopicIdPartition] = {
    val erroneousTopicIdPartitions: mutable.Set[TopicIdPartition] = mutable.Set.empty[TopicIdPartition]

    acknowledgementDataFromRequest.foreach { case (tp: TopicIdPartition, acknowledgeBatches: util.List[ShareAcknowledgementBatch]) =>
      var prevEndOffset = -1L
      var isErroneous = false
      acknowledgeBatches.forEach { batch =>
        if (!isErroneous) {
          if (batch.firstOffset > batch.lastOffset) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.firstOffset < prevEndOffset) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes == null || batch.acknowledgeTypes.isEmpty) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes.size() > 1 && batch.lastOffset - batch.firstOffset != batch.acknowledgeTypes.size() - 1) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else if (batch.acknowledgeTypes.stream().anyMatch(ackType => ackType < 0 || ackType > 3)) {
            erroneous += tp -> ShareAcknowledgeResponse.partitionResponse(tp, Errors.INVALID_REQUEST)
            erroneousTopicIdPartitions.add(tp)
            isErroneous = true
          } else {
            prevEndOffset = batch.lastOffset
          }
        }
      }
    }

    erroneousTopicIdPartitions
  }

  // the callback for processing a share fetch response.
  private def processShareFetchResponse(responsePartitionData: Map[TopicIdPartition, ShareFetchResponseData.PartitionData],
                                        request: RequestChannel.Request,
                                        topicIdNames: util.Map[Uuid, String],
                                        shareFetchContext: ShareFetchContext): ShareFetchResponse = {
    val clientId = request.header.clientId
    val versionId = request.header.apiVersion
    val shareFetchRequest = request.body[ShareFetchRequest]
    val groupId = shareFetchRequest.data.groupId
    val memberId = shareFetchRequest.data.memberId

    val nodeEndpoints = new mutable.HashMap[Int, Node]
    responsePartitionData.foreach { case(tp, partitionData) =>
      partitionData.errorCode match {
        case errCode if errCode == Errors.NOT_LEADER_OR_FOLLOWER.code | errCode == Errors.FENCED_LEADER_EPOCH.code =>
          val leaderNode = getCurrentLeader(tp.topicPartition, request.context.listenerName)
          leaderNode.node.foreach { node =>
            nodeEndpoints.put(node.id, node)
          }
          partitionData.currentLeader
            .setLeaderId(leaderNode.leaderId)
            .setLeaderEpoch(leaderNode.leaderEpoch)
        case _ =>
      }
    }
    val partitions = new util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData](responsePartitionData.asJava)

    var shareFetchResponse: ShareFetchResponse = null

    def createResponse(throttleTimeMs: Int): ShareFetchResponse = {
      val responseData = new util.LinkedHashMap[TopicIdPartition, ShareFetchResponseData.PartitionData]
      shareFetchResponse.data.responses.forEach { topicResponse =>
        topicResponse.partitions.forEach { partitionData =>
          val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicIdNames.get(topicResponse.topicId),
            partitionData.partitionIndex))
          val error = Errors.forCode(partitionData.errorCode)
          if (error != Errors.NONE)
            debug(s"Share Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
              s"on partition $tp failed due to ${error.exceptionName}")
          responseData.put(tp, getResponsePartitionData(tp, partitionData))
        }
      }

      // Prepare share fetch response
      val response =
        ShareFetchResponse.of(shareFetchResponse.error, throttleTimeMs, responseData, nodeEndpoints.values.toList.asJava,
          ShareFetchUtils.recordLockDurationMsOrDefault(groupConfigManager, groupId, config.shareGroupConfig.shareGroupRecordLockDurationMs))
      // record the bytes out metrics only when the response is being sent.
      response.data.responses.forEach { topicResponse =>
        topicResponse.partitions.forEach { data =>
          // If the topic name was not known, we will have no bytes out.
          if (topicResponse.topicId != null) {
            val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicIdNames.get(topicResponse.topicId), data.partitionIndex))
            brokerTopicStats.updateBytesOut(tp.topic, false, false, ShareFetchResponse.recordsSize(data))
          }
        }
      }
      response
    }

    // Share Fetch size used to determine throttle time is calculated here.
    // This may be slightly different from the actual response size.
    //
    // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
    // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
    // quotas. When throttled, we unrecord the recorded bandwidth quota value.
    val responseSize = shareFetchContext.responseSize(partitions, versionId)
    val timeMs = time.milliseconds()
    val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
    val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request.session, request.header.clientId(), responseSize, timeMs)

    val maxThrottleTimeMs = math.max(bandwidthThrottleTimeMs, requestThrottleTimeMs)
    if (maxThrottleTimeMs > 0) {
      request.apiThrottleTimeMs = maxThrottleTimeMs
      // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
      // from the fetch quota because we are going to return an empty response.
      quotas.fetch.unrecordQuotaSensor(request.session, request.header.clientId(), responseSize, timeMs)
      if (bandwidthThrottleTimeMs > requestThrottleTimeMs) {
        requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
      } else {
        requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
      }
      // If throttling is required, return an empty response.
      shareFetchResponse = shareFetchContext.throttleResponse(maxThrottleTimeMs)
    } else {
      // Get the actual response. This will update the fetch context.
      shareFetchResponse = shareFetchContext.updateAndGenerateResponseData(groupId, Uuid.fromString(memberId), partitions)
      val responsePartitionsSize = shareFetchResponse.data.responses.stream().mapToInt(_.partitions.size()).sum()
      trace(s"Sending Share Fetch response with partitions size=$responsePartitionsSize")
    }
    createResponse(maxThrottleTimeMs)
  }

  private def getResponsePartitionData(tp: TopicIdPartition,
                                       partitionData: ShareFetchResponseData.PartitionData): ShareFetchResponseData.PartitionData = {
    val records = ShareFetchResponse.recordsOrFail(partitionData)
    new ShareFetchResponseData.PartitionData()
      .setPartitionIndex(tp.partition)
      .setErrorCode(Errors.forCode(partitionData.errorCode).code)
      .setRecords(records)
      .setAcquiredRecords(partitionData.acquiredRecords)
      .setCurrentLeader(partitionData.currentLeader)
  }

  private def shareVersion(): ShareVersion = {
    ShareVersion.fromFeatureLevel(metadataCache.features.finalizedFeatures.getOrDefault(ShareVersion.FEATURE_NAME, 0.toShort))
  }

  private def isShareGroupProtocolEnabled: Boolean = {
    config.shareGroupConfig.isShareGroupEnabled || shareVersion().supportsShareGroups
  }

  private def updateRecordConversionStats(request: RequestChannel.Request,
                                          tp: TopicPartition,
                                          conversionStats: RecordValidationStats): Unit = {
    val conversionCount = conversionStats.numRecordsConverted
    if (conversionCount > 0) {
      request.header.apiKey match {
        case ApiKeys.PRODUCE =>
          brokerTopicStats.topicStats(tp.topic).produceMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.produceMessageConversionsRate.mark(conversionCount)
        case ApiKeys.FETCH =>
          brokerTopicStats.topicStats(tp.topic).fetchMessageConversionsRate.mark(conversionCount)
          brokerTopicStats.allTopicsStats.fetchMessageConversionsRate.mark(conversionCount)
        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
      request.messageConversionsTimeNanos = conversionStats.conversionTimeNanos
    }
    request.temporaryMemoryBytes = conversionStats.temporaryMemoryBytes
  }

  def authorizeClusterOperation(request: RequestChannel.Request, operation: AclOperation): Boolean = {
    authHelper.authorize(request.context, operation, CLUSTER, CLUSTER_NAME)
  }
}

object KafkaApis {
  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  // TODO: remove resolvedResponseData method when sizeOf can take a data object.
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse,
                                                quota: ReplicationQuotaManager): Int = {
    val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
    unconvertedResponse.data.responses().forEach(topicResponse =>
      topicResponse.partitions().forEach(partition =>
        responseData.put(new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic(), partition.partitionIndex)), partition)))
    FetchResponse.sizeOf(versionId, responseData.entrySet
      .iterator.asScala.filter(element => element.getKey.topicPartition.topic != null && quota.isThrottled(element.getKey.topicPartition)).asJava)
  }
}
