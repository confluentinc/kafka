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

package org.apache.kafka.timeline;

import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class SnapshottableHashTableTest {

    /**
     * The class of test elements.
     *
     * This class is intended to help test how the table handles distinct objects which
     * are equal to each other.  Therefore, for the purpose of hashing and equality, we
     * only check i here, and ignore j.
     */
    static class TestElement implements SnapshottableHashTable.ElementWithStartEpoch {
        private final int i;
        private final char j;
        private long startEpoch = Long.MAX_VALUE;

        TestElement(int i, char j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public void setStartEpoch(long startEpoch) {
            this.startEpoch = startEpoch;
        }

        @Override
        public long startEpoch() {
            return startEpoch;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TestElement other)) {
                return false;
            }
            return other.i == i;
        }

        @Override
        public String toString() {
            return String.format("E_%d%c(%s)", i, j, System.identityHashCode(this));
        }
    }

    private static final TestElement E_1A = new TestElement(1, 'A');
    private static final TestElement E_1B = new TestElement(1, 'B');
    private static final TestElement E_2A = new TestElement(2, 'A');
    private static final TestElement E_3A = new TestElement(3, 'A');
    private static final TestElement E_3B = new TestElement(3, 'B');

    @Test
    public void testEmptyTable() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
    }
    @Test
    public void testDeleteOnEmptyDeltaTable() {
        // A simple test case to validate the behavior of the TimelineHashSet
        // when the deltaTable for a snapshot is null
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        TimelineHashSet<String> set = new TimelineHashSet<>(registry, 5);

        registry.getOrCreateSnapshot(100);
        set.add("bar");
        registry.getOrCreateSnapshot(200);
        set.add("baz");

        // The deltatable of epoch 200 is null, it should not throw exception while reverting (deltatable merge)
        registry.revertToSnapshot(100);
        assertTrue(set.isEmpty());
        set.add("foo");
        registry.getOrCreateSnapshot(300);
        // After reverting to epoch 100, "bar" is not existed anymore
        set.remove("bar");
        // No deltatable merging is needed because nothing change in snapshot epoch 300
        registry.revertToSnapshot(100);
        assertTrue(set.isEmpty());

        set.add("qux");
        registry.getOrCreateSnapshot(400);
        assertEquals(1, set.size());
        set.add("fred");
        set.add("thud");
        registry.getOrCreateSnapshot(500);
        assertEquals(3, set.size());

        // remove the value in epoch 101(after epoch 100), it'll create an entry in deltatable in the snapshot of epoch 500 for the deleted value in epoch 101
        set.remove("qux");
        assertEquals(2, set.size());
        // When reverting to snapshot of epoch 400, we'll merge the deltatable in epoch 500 with the one in epoch 400.
        // The deltatable in epoch 500 has an entry created above, but the deltatable in epoch 400 is null.
        // It should not throw exception while reverting (deltatable merge)
        registry.revertToSnapshot(400);
        // After reverting, the deltatable in epoch 500 should merge to the current epoch
        assertEquals(1, set.size());

        // When reverting to epoch 100, the deltatable in epoch 400 won't be merged because the entry change is epoch 101(after epoch 100)
        registry.revertToSnapshot(100);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testAddAndRemove() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertNull(table.snapshottableAddOrReplace(E_1B));
        assertEquals(1, table.snapshottableSize(Long.MAX_VALUE));
        registry.getOrCreateSnapshot(0);
        assertSame(E_1B, table.snapshottableAddOrReplace(E_1A));
        assertSame(E_1B, table.snapshottableGet(E_1A, 0));
        assertSame(E_1A, table.snapshottableGet(E_1A, Long.MAX_VALUE));
        assertNull(table.snapshottableAddOrReplace(E_2A));
        assertNull(table.snapshottableAddOrReplace(E_3A));
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(1, table.snapshottableSize(0));
        registry.getOrCreateSnapshot(1);
        assertEquals(E_1A, table.snapshottableRemove(E_1B));
        assertEquals(E_2A, table.snapshottableRemove(E_2A));
        assertEquals(E_3A, table.snapshottableRemove(E_3A));
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(1, table.snapshottableSize(0));
        assertEquals(3, table.snapshottableSize(1));
        registry.deleteSnapshot(0);
        assertEquals("No in-memory snapshot for epoch 0. Snapshot epochs are: 1",
            assertThrows(RuntimeException.class, () ->
                table.snapshottableSize(0)).getMessage());
        registry.deleteSnapshot(1);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
    }

    @Test
    public void testIterateOverSnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertTrue(table.snapshottableAddUnlessPresent(E_1B));
        assertFalse(table.snapshottableAddUnlessPresent(E_1A));
        assertTrue(table.snapshottableAddUnlessPresent(E_2A));
        assertTrue(table.snapshottableAddUnlessPresent(E_3A));
        registry.getOrCreateSnapshot(0);
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
        assertEquals(E_1B, table.snapshottableRemove(E_1B));
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
        assertNull(table.snapshottableRemove(E_1A));
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_2A, E_3A);
        assertEquals(E_2A, table.snapshottableRemove(E_2A));
        assertEquals(E_3A, table.snapshottableRemove(E_3A));
        assertIteratorYields(table.snapshottableIterator(0), E_1B, E_2A, E_3A);
    }

    @Test
    public void testIterateOverSnapshotWhileExpandingTable() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertNull(table.snapshottableAddOrReplace(E_1A));
        registry.getOrCreateSnapshot(0);
        Iterator<TestElement> iter = table.snapshottableIterator(0);
        assertTrue(table.snapshottableAddUnlessPresent(E_2A));
        assertTrue(table.snapshottableAddUnlessPresent(E_3A));
        assertIteratorYields(iter, E_1A);
    }

    @Test
    public void testIterateOverSnapshotWhileDeletingAndReplacing() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertNull(table.snapshottableAddOrReplace(E_1A));
        assertNull(table.snapshottableAddOrReplace(E_2A));
        assertNull(table.snapshottableAddOrReplace(E_3A));
        assertEquals(E_1A, table.snapshottableRemove(E_1A));
        assertNull(table.snapshottableAddOrReplace(E_1B));
        registry.getOrCreateSnapshot(0);
        Iterator<TestElement> iter = table.snapshottableIterator(0);
        List<TestElement> iterElements = new ArrayList<>();
        iterElements.add(iter.next());
        assertEquals(E_2A, table.snapshottableRemove(E_2A));
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B));
        iterElements.add(iter.next());
        assertEquals(E_1B, table.snapshottableRemove(E_1B));
        iterElements.add(iter.next());
        assertFalse(iter.hasNext());
        assertIteratorYields(iterElements.iterator(), E_1B, E_2A, E_3A);
    }

    @Test
    public void testRevert() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertNull(table.snapshottableAddOrReplace(E_1A));
        assertNull(table.snapshottableAddOrReplace(E_2A));
        assertNull(table.snapshottableAddOrReplace(E_3A));
        registry.getOrCreateSnapshot(0);
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B));
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B));
        registry.getOrCreateSnapshot(1);
        assertEquals(3, table.snapshottableSize(Long.MAX_VALUE));
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_1B, E_2A, E_3B);
        table.snapshottableRemove(E_1B);
        table.snapshottableRemove(E_2A);
        table.snapshottableRemove(E_3B);
        assertEquals(0, table.snapshottableSize(Long.MAX_VALUE));
        assertEquals(3, table.snapshottableSize(0));
        assertEquals(3, table.snapshottableSize(1));
        registry.revertToSnapshot(0);
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE), E_1A, E_2A, E_3A);
    }

    @Test
    public void testReset() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
            new SnapshottableHashTable<>(registry, 1);
        assertNull(table.snapshottableAddOrReplace(E_1A));
        assertNull(table.snapshottableAddOrReplace(E_2A));
        assertNull(table.snapshottableAddOrReplace(E_3A));
        registry.getOrCreateSnapshot(0);
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B));
        assertEquals(E_3A, table.snapshottableAddOrReplace(E_3B));
        registry.getOrCreateSnapshot(1);

        registry.reset();

        assertEquals(List.of(), registry.epochsList());
        // Check that the table is empty
        assertIteratorYields(table.snapshottableIterator(Long.MAX_VALUE));
    }

    @Test
    public void testIteratorAtOlderEpoch() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        SnapshottableHashTable<TestElement> table =
                new SnapshottableHashTable<>(registry, 4);
        assertNull(table.snapshottableAddOrReplace(E_3B));
        registry.getOrCreateSnapshot(0);
        assertNull(table.snapshottableAddOrReplace(E_1A));
        registry.getOrCreateSnapshot(1);
        assertEquals(E_1A, table.snapshottableAddOrReplace(E_1B));
        registry.getOrCreateSnapshot(2);
        assertEquals(E_1B, table.snapshottableRemove(E_1B));
        assertIteratorYields(table.snapshottableIterator(1), E_3B, E_1A);
    }

    /**
     * Assert that the given iterator contains the given elements, in any order.
     * We compare using reference equality here, rather than object equality.
     */
    private static void assertIteratorYields(Iterator<?> iter,
                                             Object... expected) {
        IdentityHashMap<Object, Boolean> remaining = new IdentityHashMap<>();
        for (Object object : expected) {
            remaining.put(object, true);
        }
        List<Object> extraObjects = new ArrayList<>();
        while (iter.hasNext()) {
            Object object = iter.next();
            assertNotNull(object);
            if (remaining.remove(object) == null) {
                extraObjects.add(object);
            }
        }
        if (!extraObjects.isEmpty() || !remaining.isEmpty()) {
            throw new RuntimeException("Found extra object(s): [" + extraObjects.stream().map(Object::toString).collect(Collectors.joining(", ")) +
                "] and didn't find object(s): [" + remaining.keySet().stream().map(Object::toString).collect(Collectors.joining(", ")) + "]");
        }
    }
}
