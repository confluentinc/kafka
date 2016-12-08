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

import com.sun.org.apache.xml.internal.security.Init;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

public class InitPIDResponse extends AbstractResponse {
    public static final long INVALID_PID = -1;
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.INIT_PRODUCER_ID.id);
    private static final String PRODUCER_ID_KEY_NAME = "pid";
    private static final String EPOCH_KEY_NAME = "epoch";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private final long producerId;
    private final short epoch;
    private final short errorCode;

    public InitPIDResponse(int version, long producerId, short epoch, short errorCode) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.INIT_PRODUCER_ID.id, version)));
        this.producerId = producerId;
        this.epoch = epoch;
        this.errorCode = errorCode;
        // Stub implementation.

    }

    public InitPIDResponse(Struct struct) {
        super(struct);
        this.producerId = struct.getLong(PRODUCER_ID_KEY_NAME);
        this.epoch = struct.getShort(EPOCH_KEY_NAME);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
    }

    public InitPIDResponse(Errors errors) {
        super(new Struct(CURRENT_SCHEMA));
        this.producerId = INVALID_PID;
        this.epoch = 0;
        this.errorCode = errors.code();
    }

    public long producerId() {
        return producerId;
    }

    public short errorCode() {
        return errorCode;
    }

    public short epoch() {
        return epoch;
    }
}

