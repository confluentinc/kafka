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
package org.apache.kafka.coordinator.group.modern.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.image.MetadataImage;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShareGroupAssignmentBuilderTest {

    @Test
    public void testStableToStable() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .build();

        ShareGroupMember member = new ShareGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6))))
            .build();

        assertEquals(
            new ShareGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3),
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testStableToStableWithNewPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .build();

        ShareGroupMember member = new ShareGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic1, topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3, 4),
                mkTopicAssignment(topicId2, 4, 5, 6, 7))))
            .build();

        assertEquals(
            new ShareGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic1, topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId1, 1, 2, 3, 4),
                    mkTopicAssignment(topicId2, 4, 5, 6, 7)))
                .build(),
            updatedMember
        );
    }

    @Test
    public void testAssignmentTopicsNoLongerInSubscription() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Uuid topicId1 = Uuid.randomUuid();
        Uuid topicId2 = Uuid.randomUuid();

        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(topicId1, topic1, 10)
            .addTopic(topicId2, topic2, 10)
            .build();

        ShareGroupMember member = new ShareGroupMember.Builder("member")
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(10)
            .setSubscribedTopicNames(List.of(topic2))
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6)))
            .build();

        ShareGroupMember updatedMember = new ShareGroupAssignmentBuilder(member)
            .withMetadataImage(metadataImage)
            .withTargetAssignment(11, new Assignment(mkAssignment(
                mkTopicAssignment(topicId1, 1, 2, 3),
                mkTopicAssignment(topicId2, 4, 5, 6))))
            .build();

        assertEquals(
            new ShareGroupMember.Builder("member")
                .setState(MemberState.STABLE)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setSubscribedTopicNames(List.of(topic2))
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(topicId2, 4, 5, 6)))
                .build(),
            updatedMember
        );
    }
}
