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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Collections;

/**
 * The result of the {@link Admin#listShareGroups(ListShareGroupsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupsResult {
    /**
     * Returns a future that yields either an exception, or the full set of share group listings.
     */
    public KafkaFuture<Collection<ShareGroupListing>> all() {
        // Implementation will be done as part of future PRs in KIP-932
        return KafkaFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Returns a future which yields just the valid listings.
     */
    public KafkaFuture<Collection<ShareGroupListing>> valid() {
        // Implementation will be done as part of future PRs in KIP-932
        return KafkaFuture.completedFuture(Collections.emptyList());
    }

    /**
     * Returns a future which yields just the errors which occurred.
     */
    public KafkaFuture<Collection<Throwable>> errors() {
        // Implementation will be done as part of future PRs in KIP-932
        return KafkaFuture.completedFuture(Collections.emptyList());
    }
}