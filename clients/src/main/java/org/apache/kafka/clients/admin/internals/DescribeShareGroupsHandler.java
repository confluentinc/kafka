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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeRequestData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ShareGroupDescribeRequest;
import org.apache.kafka.common.requests.ShareGroupDescribeResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DescribeShareGroupsHandler implements AdminApiHandler<CoordinatorKey, ShareGroupDescription> {

  private final boolean includeAuthorizedOperations;
  private final Logger log;
  private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;
  private final Set<String> useClassicGroupApi;

  public DescribeShareGroupsHandler(
      boolean includeAuthorizedOperations,
      LogContext logContext
  ) {
    this.includeAuthorizedOperations = includeAuthorizedOperations;
    this.log = logContext.logger(DescribeShareGroupsHandler.class);
    this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    this.useClassicGroupApi = new HashSet<>();
  }

  private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
    return groupIds.stream()
        .map(CoordinatorKey::byGroupId)
        .collect(Collectors.toSet());
  }

  public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ShareGroupDescription> newFuture(
      Collection<String> groupIds
  ) {
    return AdminApiFuture.forKeys(buildKeySet(groupIds));
  }

  @Override
  public String apiName() {
    return "describeShareGroups";
  }

  @Override
  public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
    return lookupStrategy;
  }

  @Override
  public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
    Set<CoordinatorKey> newShareGroupKeys = new HashSet<>();
    Set<CoordinatorKey> oldShareGroupKeys = new HashSet<>();
    List<String> newShareGroupIds = new ArrayList<>();
    List<String> oldShareGroupIds = new ArrayList<>();

    keys.forEach(key -> {
      if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
        throw new IllegalArgumentException("Invalid group coordinator key " + key +
            " when building `DescribeGroups` request");
      }

      // By default, we always try using the new share group describe API.
      // If it fails, we fail back to using the classic group API.
      if (useClassicGroupApi.contains(key.idValue)) {
        oldShareGroupKeys.add(key);
        oldShareGroupIds.add(key.idValue);
      } else {
        newShareGroupKeys.add(key);
        newShareGroupIds.add(key.idValue);
      }
    });

    List<RequestAndKeys<CoordinatorKey>> requests = new ArrayList<>();
    if (!newShareGroupKeys.isEmpty()) {
      ShareGroupDescribeRequestData data = new ShareGroupDescribeRequestData()
          .setGroupIds(newShareGroupIds)
          .setIncludeAuthorizedOperations(includeAuthorizedOperations);
      requests.add(new RequestAndKeys<>(new ShareGroupDescribeRequest.Builder(data), newShareGroupKeys));
    }

    if (!oldShareGroupKeys.isEmpty()) {
      DescribeGroupsRequestData data = new DescribeGroupsRequestData()
          .setGroups(oldShareGroupIds)
          .setIncludeAuthorizedOperations(includeAuthorizedOperations);
      requests.add(new RequestAndKeys<>(new DescribeGroupsRequest.Builder(data), oldShareGroupKeys));
    }

    return requests;
  }

  private ApiResult<CoordinatorKey, ShareGroupDescription> handledClassicGroupResponse(
      Node coordinator,
      Map<CoordinatorKey, ShareGroupDescription> completed,
      Map<CoordinatorKey, Throwable> failed,
      Set<CoordinatorKey> groupsToUnmap,
      DescribeGroupsResponse response
  ) {
    for (DescribeGroupsResponseData.DescribedGroup describedGroup : response.data().groups()) {
      CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
      Errors error = Errors.forCode(describedGroup.errorCode());
      if (error != Errors.NONE) {
        handleError(
            groupIdKey,
            error,
            null,
            failed,
            groupsToUnmap,
            false
        );
        continue;
      }
      final String protocolType = describedGroup.protocolType();
      if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
        final List<DescribeGroupsResponseData.DescribedGroupMember> members = describedGroup.members();
        final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
        final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
        for (DescribeGroupsResponseData.DescribedGroupMember groupMember : members) {
          Set<TopicPartition> partitions = Collections.emptySet();
          if (groupMember.memberAssignment().length > 0) {
            final ConsumerPartitionAssignor.Assignment assignment = ConsumerProtocol.
                deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
            partitions = new HashSet<>(assignment.partitions());
          }
          memberDescriptions.add(new MemberDescription(
              groupMember.memberId(),
              Optional.ofNullable(groupMember.groupInstanceId()),
              groupMember.clientId(),
              groupMember.clientHost(),
              new MemberAssignment(partitions)));
        }
        final ShareGroupDescription shareGroupDescription =
            new ShareGroupDescription(groupIdKey.idValue,
                memberDescriptions,
                describedGroup.protocolData(),
                GroupType.CLASSIC,
                ShareGroupState.parse(describedGroup.groupState()),
                coordinator,
                authorizedOperations);
        completed.put(groupIdKey, shareGroupDescription);
      } else {
        failed.put(groupIdKey, new IllegalArgumentException(
            String.format("GroupId %s is not a consumer group (%s).",
                groupIdKey.idValue, protocolType)));
      }
    }

    return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
  }

  @Override
  public ApiResult<CoordinatorKey, ShareGroupDescription> handleResponse(
      Node coordinator,
      Set<CoordinatorKey> groupIds,
      AbstractResponse abstractResponse
  ) {
    final Map<CoordinatorKey, ShareGroupDescription> completed = new HashMap<>();
    final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
    final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

    if (abstractResponse instanceof DescribeGroupsResponse) {
      return handledClassicGroupResponse(
          coordinator,
          completed,
          failed,
          groupsToUnmap,
          (DescribeGroupsResponse) abstractResponse
      );
    } else if (abstractResponse instanceof ShareGroupDescribeResponse) {
      return handledShareGroupResponse(
          coordinator,
          completed,
          failed,
          groupsToUnmap,
          (ShareGroupDescribeResponse) abstractResponse
      );
    } else {
      throw new IllegalArgumentException("Received an unexpected response type.");
    }
  }

  @Override
  public Map<CoordinatorKey, Throwable> handleUnsupportedVersionException(
      int coordinator,
      UnsupportedVersionException exception,
      Set<CoordinatorKey> keys
  ) {
    Map<CoordinatorKey, Throwable> errors = new HashMap<>();

    keys.forEach(key -> {
      if (!useClassicGroupApi.add(key.idValue)) {
        // We already tried with the classic group API so we need to fail now.
        errors.put(key, exception);
      }
    });

    return errors;
  }

  private ApiResult<CoordinatorKey, ShareGroupDescription> handledShareGroupResponse(
      Node coordinator,
      Map<CoordinatorKey, ShareGroupDescription> completed,
      Map<CoordinatorKey, Throwable> failed,
      Set<CoordinatorKey> groupsToUnmap,
      ShareGroupDescribeResponse response
  ) {
    for (ShareGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
      final CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
      final Errors error = Errors.forCode(describedGroup.errorCode());
      if (error != Errors.NONE) {
        handleError(
            groupIdKey,
            error,
            describedGroup.errorMessage(),
            failed,
            groupsToUnmap,
            true
        );
        continue;
      }

      final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
      final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());

      describedGroup.members().forEach(groupMember -> {
        memberDescriptions.add(new MemberDescription(
            groupMember.memberId(),
            Optional.ofNullable(groupMember.instanceId()),
            groupMember.clientId(),
            groupMember.clientHost(),
            new MemberAssignment(convertAssignment(groupMember.assignment())),
            Optional.of(new MemberAssignment(convertAssignment(groupMember.assignment())))
        ));
      });

      final ShareGroupDescription shareGroupDescription =
          new ShareGroupDescription(
              groupIdKey.idValue,
              memberDescriptions,
              describedGroup.assignorName(),
              GroupType.CONSUMER,
              ShareGroupState.parse(describedGroup.groupState()),
              coordinator,
              authorizedOperations
          );
      completed.put(groupIdKey, shareGroupDescription);
    }

    return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
  }

  private Set<TopicPartition> convertAssignment(ShareGroupDescribeResponseData.Assignment assignment) {
    return assignment.topicPartitions().stream().flatMap(topic ->
        topic.partitions().stream().map(partition ->
            new TopicPartition(topic.topicName(), partition)
        )
    ).collect(Collectors.toSet());
  }

  private void handleError(
      CoordinatorKey groupId,
      Errors error,
      String errorMsg,
      Map<CoordinatorKey, Throwable> failed,
      Set<CoordinatorKey> groupsToUnmap,
      boolean isShareGroupResponse
  ) {
    String apiName = isShareGroupResponse ? "ShareGroupDescribe" : "DescribeGroups";

    switch (error) {
      case GROUP_AUTHORIZATION_FAILED:
        log.debug("`{}` request for group id {} failed due to error {}.", apiName, groupId.idValue, error);
        failed.put(groupId, error.exception(errorMsg));
        break;

      case COORDINATOR_LOAD_IN_PROGRESS:
        // If the coordinator is in the middle of loading, then we just need to retry
        log.debug("`{}` request for group id {} failed because the coordinator " +
            "is still in the process of loading state. Will retry.", apiName, groupId.idValue);
        break;

      case COORDINATOR_NOT_AVAILABLE:
      case NOT_COORDINATOR:
        // If the coordinator is unavailable or there was a coordinator change, then we unmap
        // the key so that we retry the `FindCoordinator` request
        log.debug("`{}` request for group id {} returned error {}. " +
            "Will attempt to find the coordinator again and retry.", apiName, groupId.idValue, error);
        groupsToUnmap.add(groupId);
        break;

      case UNSUPPORTED_VERSION:
        if (isShareGroupResponse) {
          log.debug("`{}` request for group id {} failed because the API is not " +
              "supported. Will retry with `DescribeGroups` API.", apiName, groupId.idValue);
          useClassicGroupApi.add(groupId.idValue);
        } else {
          log.error("`{}` request for group id {} failed because the `ShareGroupDescribe` API is not supported.",
              apiName, groupId.idValue);
          failed.put(groupId, error.exception(errorMsg));
        }
        break;

      case GROUP_ID_NOT_FOUND:
        if (isShareGroupResponse) {
          log.debug("`{}` request for group id {} failed because the group is not " +
              "a new share group. Will retry with `DescribeGroups` API.", apiName, groupId.idValue);
          useClassicGroupApi.add(groupId.idValue);
        } else {
          log.error("`{}` request for group id {} failed because the group does not exist.", apiName, groupId.idValue);
          failed.put(groupId, error.exception(errorMsg));
        }
        break;

      default:
        log.error("`{}` request for group id {} failed due to unexpected error {}.", apiName, groupId.idValue, error);
        failed.put(groupId, error.exception(errorMsg));
    }
  }

  private Set<AclOperation> validAclOperations(final int authorizedOperations) {
    if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) {
      return null;
    }
    return Utils.from32BitField(authorizedOperations)
        .stream()
        .map(AclOperation::fromCode)
        .filter(operation -> operation != AclOperation.UNKNOWN
            && operation != AclOperation.ALL
            && operation != AclOperation.ANY)
        .collect(Collectors.toSet());
  }

}
