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
import org.apache.kafka.common.protocol.types.Struct;

public class InitPIDResponse extends AbstractRequestResponse {
    private static final String PRODUCER_ID_KEY_NAME = "pid";

    // The 'String' type is temporary.
    private final String producerId;

    public InitPIDResponse(int version, String producerId) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.INIT_PRODUCER_ID.id, version)));
        this.producerId = producerId;
        struct.set(PRODUCER_ID_KEY_NAME, producerId);
        // Stub implementation.

    }

    public InitPIDResponse(Struct struct) {
        super(struct);
        this.producerId = struct.getString(PRODUCER_ID_KEY_NAME);
    }

    public String producerId() {
        return producerId;
    }

}
