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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AppInfoParserTest {
    private static final String EXPECTED_COMMIT_VERSION = AppInfoParser.DEFAULT_VALUE;
    private static final String EXPECTED_VERSION = AppInfoParser.DEFAULT_VALUE;
    private static final Long EXPECTED_START_MS = 1552313875722L;
    private static final String METRICS_PREFIX = "app-info-test";
    private static final String METRICS_ID = "test";

    private Metrics metrics;
    private MBeanServer mBeanServer;

    @BeforeEach
    public void setUp() {
        metrics = new Metrics(new MockTime(1));
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testRegisterAppInfoRegistersMetrics() throws JMException {
        registerAppInfo();
        registerAppInfoMultipleTimes();
    }

    @Test
    public void testUnregisterAppInfoUnregistersMetrics() throws JMException {
        registerAppInfo();
        AppInfoParser.unregisterAppInfo(METRICS_PREFIX, METRICS_ID, metrics);

        assertFalse(mBeanServer.isRegistered(expectedAppObjectName()));
        assertNull(metrics.metric(metrics.metricName("commit-id", "app-info")));
        assertNull(metrics.metric(metrics.metricName("version", "app-info")));
        assertNull(metrics.metric(metrics.metricName("start-time-ms", "app-info")));
    }

    private void registerAppInfo() throws JMException {
        assertEquals(EXPECTED_COMMIT_VERSION, AppInfoParser.getCommitId());
        assertEquals(EXPECTED_VERSION, AppInfoParser.getVersion());

        AppInfoParser.registerAppInfo(METRICS_PREFIX, METRICS_ID, metrics, EXPECTED_START_MS);

        assertTrue(mBeanServer.isRegistered(expectedAppObjectName()));
        assertEquals(EXPECTED_COMMIT_VERSION, metrics.metric(metrics.metricName("commit-id", "app-info")).metricValue());
        assertEquals(EXPECTED_VERSION, metrics.metric(metrics.metricName("version", "app-info")).metricValue());
        assertEquals(EXPECTED_START_MS, metrics.metric(metrics.metricName("start-time-ms", "app-info")).metricValue());
    }

    private void registerAppInfoMultipleTimes() throws JMException {
        assertEquals(EXPECTED_COMMIT_VERSION, AppInfoParser.getCommitId());
        assertEquals(EXPECTED_VERSION, AppInfoParser.getVersion());

        AppInfoParser.registerAppInfo(METRICS_PREFIX, METRICS_ID, metrics, EXPECTED_START_MS);
        AppInfoParser.registerAppInfo(METRICS_PREFIX, METRICS_ID, metrics, EXPECTED_START_MS); // We register it again

        assertTrue(mBeanServer.isRegistered(expectedAppObjectName()));
        assertEquals(EXPECTED_COMMIT_VERSION, metrics.metric(metrics.metricName("commit-id", "app-info")).metricValue());
        assertEquals(EXPECTED_VERSION, metrics.metric(metrics.metricName("version", "app-info")).metricValue());
        assertEquals(EXPECTED_START_MS, metrics.metric(metrics.metricName("start-time-ms", "app-info")).metricValue());
    }

    private ObjectName expectedAppObjectName() throws MalformedObjectNameException {
        return new ObjectName(METRICS_PREFIX + ":type=app-info,id=" + METRICS_ID);
    }
}
