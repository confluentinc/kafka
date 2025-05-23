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

import org.apache.kafka.common.message.UpdateRaftVoterRequestData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class    UpdateRaftVoterRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<UpdateRaftVoterRequest> {
        private final UpdateRaftVoterRequestData data;

        public Builder(UpdateRaftVoterRequestData data) {
            super(ApiKeys.UPDATE_RAFT_VOTER);
            this.data = data;
        }

        @Override
        public UpdateRaftVoterRequest build(short version) {
            return new UpdateRaftVoterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

    }

    private final UpdateRaftVoterRequestData data;

    public UpdateRaftVoterRequest(UpdateRaftVoterRequestData data, short version) {
        super(ApiKeys.UPDATE_RAFT_VOTER, version);
        this.data = data;
    }

    @Override
    public UpdateRaftVoterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new UpdateRaftVoterResponse(new UpdateRaftVoterResponseData().
            setErrorCode(Errors.forException(e).code()).
            setThrottleTimeMs(throttleTimeMs));
    }

    public static UpdateRaftVoterRequest parse(Readable readable, short version) {
        return new UpdateRaftVoterRequest(
            new UpdateRaftVoterRequestData(readable, version),
            version);
    }
}
