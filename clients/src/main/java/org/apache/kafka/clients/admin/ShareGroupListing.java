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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.ShareGroupState;

import java.util.Optional;

/**
 * A listing of a share group in the cluster.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ShareGroupListing {
    public ShareGroupListing(String groupId) {}

    public ShareGroupListing(String groupId, Optional<ShareGroupState> state) {}

    /**
     * The id of the share group.
     */
    public String groupId() {
        // Implementation will be done as part of future PRs in KIP-932
        return null;
    }

    /**
     * The share group state.
     */
    public Optional<ShareGroupState> state() {
        // Implementation will be done as part of future PRs in KIP-932
        return Optional.empty();
    }
}