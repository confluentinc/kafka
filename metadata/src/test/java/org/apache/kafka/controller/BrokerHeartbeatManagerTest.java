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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.BrokerHeartbeatManager.BrokerHeartbeatState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class BrokerHeartbeatManagerTest {
    private static BrokerHeartbeatManager newBrokerHeartbeatManager() {
        LogContext logContext = new LogContext();
        MockTime time = new MockTime(0, 1000000, 0);
        return new BrokerHeartbeatManager(logContext, time,
            TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testHasValidSession() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0));
        manager.touch(0, false);
        time.sleep(5);
        manager.touch(1, false);
        manager.touch(2, false);
        assertTrue(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        time.sleep(6);
        assertFalse(manager.hasValidSession(0));
        assertTrue(manager.hasValidSession(1));
        assertTrue(manager.hasValidSession(2));
        assertFalse(manager.hasValidSession(3));
        manager.remove(2);
        assertFalse(manager.hasValidSession(2));
        manager.remove(1);
        assertFalse(manager.hasValidSession(1));
    }

    @Test
    public void testMaybeFenceLeastRecentlyContacted() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertFalse(manager.hasValidSession(0));
        manager.touch(0, false);
        time.sleep(5);
        manager.touch(1, false);
        time.sleep(1);
        manager.touch(2, false);

        Iterator<BrokerHeartbeatState> iter = manager.unfenced().iterator();
        assertEquals(0, iter.next().id());
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());

        time.sleep(5);
        assertEquals(0, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());
        iter = manager.unfenced().iterator();
        assertEquals(1, iter.next().id());
        assertEquals(2, iter.next().id());
        assertFalse(iter.hasNext());

        time.sleep(20);
        assertEquals(1, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(2, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(-1, manager.maybeFenceLeastRecentlyContacted());
        iter = manager.unfenced().iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    public void testNextCheckTimeNs() {
        BrokerHeartbeatManager manager = newBrokerHeartbeatManager();
        MockTime time = (MockTime)  manager.time();
        assertEquals(Long.MAX_VALUE, manager.nextCheckTimeNs());
        manager.touch(0, false);
        time.sleep(1);
        manager.touch(1, false);
        time.sleep(1);
        manager.touch(2, false);
        time.sleep(1);
        manager.touch(3, false);
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        time.sleep(20);
        assertEquals(10_000_000, manager.nextCheckTimeNs());
        assertEquals(0, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(11_000_000, manager.nextCheckTimeNs());
        assertEquals(1, manager.maybeFenceLeastRecentlyContacted());
        assertEquals(12_000_000, manager.nextCheckTimeNs());
    }
}
