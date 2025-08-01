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
package org.apache.kafka.common.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtocolTest {

    @Test
    public void testToHtml() {
        var html = Protocol.toHtml();
        assertFalse(html.isBlank());
        assertFalse(html.contains("LeaderAndIsr"), "Removed LeaderAndIsr should not show in HTML");

        String requestVersion;
        String responseVersion;
        for (ApiKeys key : ApiKeys.clientApis()) {
            for (short version = key.oldestVersion(); version <= key.latestVersion(); version++) {
                requestVersion = key.name + " Request (Version: " + version;
                responseVersion = key.name + " Response (Version: " + version;

                assertTrue(html.contains(requestVersion), "Missing request header for " + key.name + " version:" + version);
                assertTrue(html.contains(responseVersion), "Missing response header for " + key.name + " version:" + version);
            }
        }
    }

}
