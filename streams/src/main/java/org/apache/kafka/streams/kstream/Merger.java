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
package org.apache.kafka.streams.kstream;


/**
 * The interface for merging aggregate values for {@link SessionWindows} with the given key.
 *
 * @param <K>   key type
 * @param <V>   aggregate value type
 */
@FunctionalInterface
public interface Merger<K, V> {

    /**
     * Compute a new aggregate from the key and two aggregates.
     *
     * @param aggKey    the key of the record
     * @param aggOne    the first aggregate
     * @param aggTwo    the second aggregate
     * @return          the new aggregate value
     */
    V apply(final K aggKey, final V aggOne, final V aggTwo);
}
