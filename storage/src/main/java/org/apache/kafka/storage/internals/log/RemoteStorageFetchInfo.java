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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchIsolation;

public class RemoteStorageFetchInfo {

    public final int fetchMaxBytes;
    public final boolean minOneMessage;
    public final TopicIdPartition topicIdPartition;
    public final FetchRequest.PartitionData fetchInfo;
    public final FetchIsolation fetchIsolation;

    public RemoteStorageFetchInfo(int fetchMaxBytes, boolean minOneMessage, TopicIdPartition topicIdPartition,
                                  FetchRequest.PartitionData fetchInfo, FetchIsolation fetchIsolation) {
        this.fetchMaxBytes = fetchMaxBytes;
        this.minOneMessage = minOneMessage;
        this.topicIdPartition = topicIdPartition;
        this.fetchInfo = fetchInfo;
        this.fetchIsolation = fetchIsolation;
    }

    @Override
    public String toString() {
        return "RemoteStorageFetchInfo{" +
                "fetchMaxBytes=" + fetchMaxBytes +
                ", minOneMessage=" + minOneMessage +
                ", topicIdPartition=" + topicIdPartition +
                ", fetchInfo=" + fetchInfo +
                ", fetchIsolation=" + fetchIsolation +
                '}';
    }
}
