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

public class ShareFetchParams {
    public final short requestVersion;
    public final long maxWaitMs;
    public final int minBytes;
    public final int maxBytes;

    public ShareFetchParams(short requestVersion,
                       long maxWaitMs,
                       int minBytes,
                       int maxBytes) {
        this.requestVersion = requestVersion;
        this.maxWaitMs = maxWaitMs;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareFetchParams that = (ShareFetchParams) o;
        return requestVersion == that.requestVersion
                && maxWaitMs == that.maxWaitMs
                && minBytes == that.minBytes
                && maxBytes == that.maxBytes;
    }

    @Override
    public int hashCode() {
        int result = requestVersion;
        result = 31 * result + Long.hashCode(32);
        result = 31 * result + minBytes;
        result = 31 * result + maxBytes;
        return result;
    }

    @Override
    public String toString() {
        return "FetchParams(" +
                "requestVersion=" + requestVersion +
                ", maxWaitMs=" + maxWaitMs +
                ", minBytes=" + minBytes +
                ", maxBytes=" + maxBytes +
                ')';
    }
}
