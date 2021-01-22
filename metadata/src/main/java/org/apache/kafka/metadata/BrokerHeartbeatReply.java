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

import java.util.Objects;


public class BrokerHeartbeatReply {
    private final boolean isCaughtUp;
    private final boolean isFenced;
    private final boolean shouldShutdown;

    public BrokerHeartbeatReply(boolean isCaughtUp, boolean isFenced,
                                boolean shouldShutdown) {
        this.isCaughtUp = isCaughtUp;
        this.isFenced = isFenced;
        this.shouldShutdown = shouldShutdown;
    }

    public boolean isCaughtUp() {
        return isCaughtUp;
    }

    public boolean isFenced() {
        return isFenced;
    }

    public boolean shouldShutdown() {
        return shouldShutdown;
    }

    @Override
    public int hashCode() {
        return Objects.hash(isCaughtUp, isFenced, shouldShutdown);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BrokerHeartbeatReply)) return false;
        BrokerHeartbeatReply other = (BrokerHeartbeatReply) o;
        return other.isCaughtUp == isCaughtUp &&
            other.isFenced == isFenced &&
            other.shouldShutdown == shouldShutdown;
    }

    @Override
    public String toString() {
        return "BrokerHeartbeatReply(isCaughtUp=" + isCaughtUp +
            ", isFenced=" + isFenced +
            ", shouldShutdown = " + shouldShutdown + ")";
    }
}
