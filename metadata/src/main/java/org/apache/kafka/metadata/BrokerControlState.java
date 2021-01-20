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

package org.apache.kafka.metadata;


/**
 * The state of a broker on the controller.
 */
public enum BrokerControlState {
    /**
     * The broker is fenced.
     *
     * In this state, the broker can be selected to host new partitions that are
     * manually assigned.  It cannot be in any ISRs.  It will not be present in
     * MetadataResponses.
     */
    FENCED,

    /**
     * The broker is unfenced and not shutting down.
     *
     * In this state, the broker can become a leader, be a member of ISRs, and
     * be selected to host new partitions that are randomly assigned.  It will
     * be present in MetadataResponses.
     */
    ACTIVE,

    /**
     * The broker is unfenced but shutting down.
     *
     * The broker will be selected to become a leader only if there are no other
     * choices.  It will not be selected to host new partitions that are
     * randomly assigned, but can be manually assigned a partition.  It will be
     * present in MetadataResponses.
     */
    CONTROLLED_SHUTDOWN;
}
