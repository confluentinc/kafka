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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collection;

/**
 * The result of the {@link Admin#listConfigResources()} call.
 * <p>
 */
public class ListConfigResourcesResult {
    private final KafkaFuture<Collection<ConfigResource>> future;

    ListConfigResourcesResult(KafkaFuture<Collection<ConfigResource>> future) {
        this.future = future;
    }

    /**
     * Returns a future that yields either an exception, or the full set of config resources.
     *
     * In the event of a failure, the future yields nothing but the first exception which
     * occurred.
     */
    public KafkaFuture<Collection<ConfigResource>> all() {
        final KafkaFutureImpl<Collection<ConfigResource>> result = new KafkaFutureImpl<>();
        future.whenComplete((resources, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                result.complete(resources);
            }
        });
        return result;
    }
}
