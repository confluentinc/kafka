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

import org.apache.kafka.common.message.FetchResponseData;

import java.util.Optional;

/**
 * Structure used for lower level reads using {@link kafka.cluster.Partition#fetchRecords()}.
 */
public record LogReadInfo(FetchDataInfo fetchedData, Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
                          long highWatermark, long logStartOffset, long logEndOffset, long lastStableOffset) {

    @Override
    public String toString() {
        return "LogReadInfo(" +
                "fetchedData=" + fetchedData +
                ", divergingEpoch=" + divergingEpoch +
                ", highWatermark=" + highWatermark +
                ", logStartOffset=" + logStartOffset +
                ", logEndOffset=" + logEndOffset +
                ", lastStableOffset=" + lastStableOffset +
                ')';
    }
}
