/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class InitPIDRequest extends AbstractRequest {
    private static final String APP_ID_KEY_NAME = "appid";

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.INIT_PRODUCER_ID.id);

    private final String appId;

    public static class Builder extends AbstractRequest.Builder<InitPIDRequest> {
        private String appId;
        public Builder(String appId) {
            super(ApiKeys.API_VERSIONS);
            this.appId = appId;
        }

        public void setAppId(String appId) {
            this.appId = appId;
        }

        @Override
        public InitPIDRequest build() {
            return new InitPIDRequest(version(), this.appId);

        }

        @Override
        public String toString() {
            return "(type=InitPIDRequest)";
        }

    }

    public InitPIDRequest(Struct struct, short versionId) {
        super(struct, versionId);
        this.appId = struct.getString(APP_ID_KEY_NAME);
    }

    private InitPIDRequest(short version, String appId) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.INIT_PRODUCER_ID.id, version)), version);
        this.appId = appId;
        struct.set(APP_ID_KEY_NAME, appId);
    }

    private InitPIDRequest(String appId) {
        this((short)0, appId);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        return new InitPIDResponse(Errors.forException(e));
    }

    public static InitPIDRequest parse(ByteBuffer buffer, int versionId) {
        return new InitPIDRequest(ProtoUtils.parseRequest(ApiKeys.INIT_PRODUCER_ID.id, versionId, buffer), (short) versionId);
    }

    public static InitPIDRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.INIT_PRODUCER_ID.id));
    }

    public String appId() {
        return  appId;
    }

}
