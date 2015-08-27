/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streaming.kstream.internals;

import org.apache.kafka.streaming.processor.Processor;
import org.apache.kafka.streaming.kstream.ValueMapper;
import org.apache.kafka.streaming.processor.ProcessorDef;

class KStreamFlatMapValues<K1, V1, V2> implements ProcessorDef {

    private final ValueMapper<V1, ? extends Iterable<V2>> mapper;

    @SuppressWarnings("unchecked")
    KStreamFlatMapValues(ValueMapper<V1, ? extends Iterable<V2>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor build() {
        return new KStreamFlatMapValuesProcessor();
    }

    private class KStreamFlatMapValuesProcessor extends KStreamProcessor<K1, V1> {
        @Override
        public void process(K1 key, V1 value) {
            Iterable<V2> newValues = mapper.apply(value);
            for (V2 v : newValues) {
                context.forward(key, v);
            }
        }
    }
}
