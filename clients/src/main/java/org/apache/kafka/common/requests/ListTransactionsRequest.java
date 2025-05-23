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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class ListTransactionsRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ListTransactionsRequest> {
        public final ListTransactionsRequestData data;

        public Builder(ListTransactionsRequestData data) {
            super(ApiKeys.LIST_TRANSACTIONS);
            this.data = data;
        }

        @Override
        public ListTransactionsRequest build(short version) {
            if (data.durationFilter() >= 0 && version < 1) {
                throw new UnsupportedVersionException("Duration filter can be set only when using API version 1 or higher." +
                        " If client is connected to an older broker, do not specify duration filter or set duration filter to -1.");
            }
            if (data.transactionalIdPattern() != null && version < 2) {
                throw new UnsupportedVersionException("Transactional ID pattern filter can be set only when using API version 2 or higher." +
                    " If client is connected to an older broker, do not specify the pattern filter.");
            }
            return new ListTransactionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ListTransactionsRequestData data;

    private ListTransactionsRequest(ListTransactionsRequestData data, short version) {
        super(ApiKeys.LIST_TRANSACTIONS, version);
        this.data = data;
    }

    public ListTransactionsRequestData data() {
        return data;
    }

    @Override
    public ListTransactionsResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        ListTransactionsResponseData response = new ListTransactionsResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(throttleTimeMs);
        return new ListTransactionsResponse(response);
    }

    public static ListTransactionsRequest parse(Readable readable, short version) {
        return new ListTransactionsRequest(new ListTransactionsRequestData(
            readable, version), version);
    }

    @Override
    public String toString(boolean verbose) {
        return data.toString();
    }

}
