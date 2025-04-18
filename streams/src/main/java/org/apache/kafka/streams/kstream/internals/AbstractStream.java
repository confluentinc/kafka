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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueJoinerWithKey;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.internals.graph.GraphNode;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/*
 * Any classes (KTable, KStream, etc) extending this class should follow the serde specification precedence ordering as:
 *
 * 1) Overridden values via control objects (e.g. Materialized, Serialized, Consumed, etc)
 * 2) Serdes that can be inferred from the operator itself (e.g. groupBy().count(), where value serde can default to `LongSerde`).
 * 3) Serde inherited from parent operator if possible (note if the key / value types have been changed, then the corresponding serde cannot be inherited).
 * 4) Default serde specified in the config.
 */
public abstract class AbstractStream<K, V> {

    protected final String name;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final Set<String> subTopologySourceNodes;
    protected final GraphNode graphNode;
    protected final InternalStreamsBuilder builder;

    // This copy-constructor will allow to extend KStream
    // and KTable APIs with new methods without impacting the public interface.
    public AbstractStream(final AbstractStream<K, V> stream) {
        this.name = stream.name;
        this.builder = stream.builder;
        this.keySerde = stream.keySerde;
        this.valueSerde = stream.valueSerde;
        this.subTopologySourceNodes = stream.subTopologySourceNodes;
        this.graphNode = stream.graphNode;
    }

    AbstractStream(final String name,
                   final Serde<K> keySerde,
                   final Serde<V> valueSerde,
                   final Set<String> subTopologySourceNodes,
                   final GraphNode graphNode,
                   final InternalStreamsBuilder builder) {
        if (subTopologySourceNodes == null || subTopologySourceNodes.isEmpty()) {
            throw new IllegalArgumentException("parameter <sourceNodes> must not be null or empty");
        }

        this.name = name;
        this.builder = builder;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.subTopologySourceNodes = subTopologySourceNodes;
        this.graphNode = graphNode;
    }

    // This method allows to expose the InternalTopologyBuilder instance
    // to subclasses that extend AbstractStream class.
    protected InternalTopologyBuilder internalTopologyBuilder() {
        return builder.internalTopologyBuilder;
    }

    Set<String> ensureCopartitionWith(final Collection<? extends AbstractStream<K, ?>> otherStreams) {
        final Set<String> allSourceNodes = new HashSet<>(subTopologySourceNodes);
        for (final AbstractStream<K, ?> other: otherStreams) {
            allSourceNodes.addAll(other.subTopologySourceNodes);
        }
        builder.internalTopologyBuilder.copartitionSources(allSourceNodes);

        return allSourceNodes;
    }

    static <VRight, VLeft, VOut> ValueJoiner<VRight, VLeft, VOut> reverseJoiner(final ValueJoiner<VLeft, VRight, VOut> joiner) {
        return (value2, value1) -> joiner.apply(value1, value2);
    }

    static <K, VRight, VLeft, VOut> ValueJoinerWithKey<K, VRight, VLeft, VOut> reverseJoinerWithKey(final ValueJoinerWithKey<K, VLeft, VRight, VOut> joiner) {
        return (key, value2, value1) -> joiner.apply(key, value1, value2);
    }

    static <K, V, VOut> ValueMapperWithKey<K, V, VOut> withKey(final ValueMapper<V, VOut> valueMapper) {
        Objects.requireNonNull(valueMapper, "valueMapper cannot be null");
        return (readOnlyKey, value) -> valueMapper.apply(value);
    }

    static <K, VLeft, VRight, VOut> ValueJoinerWithKey<K, VLeft, VRight, VOut> toValueJoinerWithKey(final ValueJoiner<VLeft, VRight, VOut> valueJoiner) {
        Objects.requireNonNull(valueJoiner, "joiner cannot be null");
        return (readOnlyKey, value1, value2) -> valueJoiner.apply(value1, value2);
    }

    // for testing only
    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }
}
