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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

public class InitPIDRequest extends AbstractRequest {
    private static final String APP_ID_KEY_NAME = "appid";

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentRequestSchema(ApiKeys.INIT_PRODUCER_ID.id);

    private final String appId;

    public InitPIDRequest(Struct struct) {
        super(struct);
        this.appId = struct.getString(APP_ID_KEY_NAME);
    }

    public InitPIDRequest(int version, String appId) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.INIT_PRODUCER_ID.id, version)));
        this.appId = appId;
        struct.set(APP_ID_KEY_NAME, appId);
    }

    public InitPIDRequest(String appId) {
        this(0, appId);
    }

    @Override
    public AbstractResponse getErrorResponse(int versionId, Throwable e) {
        return null;

    }

    public String appId() {
        return  appId;
    }
}
