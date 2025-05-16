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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.StreamsRebalanceData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DefaultStreamsRebalanceListenerTest {
    private final TaskManager taskManager = mock(TaskManager.class);
    private final StreamThread streamThread = mock(StreamThread.class);
    private DefaultStreamsRebalanceListener defaultStreamsRebalanceListener = new DefaultStreamsRebalanceListener(
            LoggerFactory.getLogger(DefaultStreamsRebalanceListener.class),
            new MockTime(),
            mock(StreamsRebalanceData.class),
            streamThread,
            taskManager
    );

    private void createRebalanceListenerWithRebalanceData(final StreamsRebalanceData streamsRebalanceData) {
        defaultStreamsRebalanceListener = new DefaultStreamsRebalanceListener(
            LoggerFactory.getLogger(DefaultStreamsRebalanceListener.class),
            new MockTime(),
            streamsRebalanceData,
            streamThread,
            taskManager
        );
    }

    @ParameterizedTest
    @EnumSource(StreamThread.State.class)
    void testOnTasksRevoked(final StreamThread.State state) {
        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));
        when(streamThread.state()).thenReturn(state);

        final Optional<Exception> result = defaultStreamsRebalanceListener.onTasksRevoked(
            Set.of(new StreamsRebalanceData.TaskId("1", 0))
        );

        assertTrue(result.isEmpty());

        final InOrder inOrder = inOrder(taskManager, streamThread);
        inOrder.verify(taskManager).handleRevocation(
            Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))
        );
        inOrder.verify(streamThread).state();
        if (state != StreamThread.State.PENDING_SHUTDOWN) {
            inOrder.verify(streamThread).setState(StreamThread.State.PARTITIONS_REVOKED);
        } else {
            inOrder.verify(streamThread, never()).setState(StreamThread.State.PARTITIONS_REVOKED);
        }
    }

    @Test
    void testOnTasksRevokedWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleRevocation(any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));

        final Optional<Exception> result = defaultStreamsRebalanceListener.onTasksRevoked(Set.of());

        assertTrue(result.isPresent());
        verify(taskManager).handleRevocation(any());
        verify(streamThread, never()).setState(any());
    }

    @Test
    void testOnTasksAssigned() {
        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(
            UUID.randomUUID(),
            Optional.empty(),
            Map.of(
                "1",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source1"),
                    Set.of(),
                    Map.of("repartition1", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                ),
                "2",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source2"),
                    Set.of(),
                    Map.of("repartition2", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                ),
                "3",
                new StreamsRebalanceData.Subtopology(
                    Set.of("source3"),
                    Set.of(),
                    Map.of("repartition3", new StreamsRebalanceData.TopicInfo(Optional.of(1), Optional.of((short) 1), Map.of())),
                    Map.of(),
                    Set.of()
                )
            ),
            Map.of()
        ));

        final Optional<Exception> result = defaultStreamsRebalanceListener.onTasksAssigned(
            new StreamsRebalanceData.Assignment(
                Set.of(new StreamsRebalanceData.TaskId("1", 0)),
                Set.of(new StreamsRebalanceData.TaskId("2", 0)),
                Set.of(new StreamsRebalanceData.TaskId("3", 0))
            )
        );

        assertTrue(result.isEmpty());

        final InOrder inOrder = inOrder(taskManager, streamThread);
        inOrder.verify(taskManager).handleAssignment(
            Map.of(new TaskId(1, 0), Set.of(new TopicPartition("source1", 0), new TopicPartition("repartition1", 0))),
            Map.of(
                new TaskId(2, 0), Set.of(new TopicPartition("source2", 0), new TopicPartition("repartition2", 0)),
                new TaskId(3, 0), Set.of(new TopicPartition("source3", 0), new TopicPartition("repartition3", 0))
            )
        );
        inOrder.verify(streamThread).setState(StreamThread.State.PARTITIONS_ASSIGNED);
        inOrder.verify(taskManager).handleRebalanceComplete();
    }

    @Test
    void testOnTasksAssignedWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleAssignment(any(), any());

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));
        final Optional<Exception> result = defaultStreamsRebalanceListener.onTasksAssigned(new StreamsRebalanceData.Assignment(Set.of(), Set.of(), Set.of()));
        assertTrue(defaultStreamsRebalanceListener.onAllTasksLost().isEmpty());
        assertTrue(result.isPresent());
        assertEquals(exception, result.get());
        verify(taskManager).handleLostAll();
        verify(streamThread, never()).setState(StreamThread.State.PARTITIONS_ASSIGNED);
        verify(taskManager, never()).handleRebalanceComplete();
    }

    @Test
    void testOnAllTasksLost() {
        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));
        assertTrue(defaultStreamsRebalanceListener.onAllTasksLost().isEmpty());
        verify(taskManager).handleLostAll();
    }

    @Test
    void testOnAllTasksLostWithException() {
        final Exception exception = new RuntimeException("sample exception");
        doThrow(exception).when(taskManager).handleLostAll();

        createRebalanceListenerWithRebalanceData(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()));
        final Optional<Exception> result = defaultStreamsRebalanceListener.onAllTasksLost();
        assertTrue(result.isPresent());
        assertEquals(exception, result.get());
        verify(taskManager).handleLostAll();
    }
}
