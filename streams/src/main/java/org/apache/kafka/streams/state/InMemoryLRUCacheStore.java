/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * An in-memory key-value store that is limited in size and retains a maximum number of most recently used entries.
 *
 * @param <K> The key type
 * @param <V> The value type
 *
 */
public class InMemoryLRUCacheStore<K, V> extends MeteredKeyValueStore<K, V> {

    /**
     * Create an in-memory key value store that records changes to a Kafka topic and the system time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be a power of 2
     * @param context the processing context
     * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity,
                                                            ProcessorContext context, Class<K> keyClass, Class<V> valueClass) {
        return create(name, capacity, context, Serdes.withBuiltinTypes(name, keyClass, valueClass), new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic and the given time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be one less than a power of 2
     * @param context the processing context
     * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param time the time provider; may not be null
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity,
                                                            ProcessorContext context, Class<K> keyClass, Class<V> valueClass,
                                                            Time time) {
        return create(name, capacity, context, Serdes.withBuiltinTypes(name, keyClass, valueClass), time);
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the {@link ProcessorContext}'s default
     * serializers and deserializers, and the system time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be one less than a power of 2
     * @param context the processing context
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity, ProcessorContext context) {
        return create(name, capacity, context, new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the {@link ProcessorContext}'s default
     * serializers and deserializers, and the given time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be one less than a power of 2
     * @param context the processing context
     * @param time the time provider; may not be null
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity, ProcessorContext context, Time time) {
        return create(name, capacity, context, new Serdes<K, V>(name, context), time);
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the provided serializers and deserializers, and
     * the system time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be one less than a power of 2
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity, ProcessorContext context,
                                                            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        return create(name, capacity, context, keySerializer, keyDeserializer, valueSerializer, valueDeserializer, new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the provided serializers and deserializers, and
     * the given time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param capacity the maximum capacity of the LRU cache; should be one less than a power of 2
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @param time the time provider; may not be null
     * @return the key-value store
     */
    public static <K, V> InMemoryLRUCacheStore<K, V> create(String name, int capacity, ProcessorContext context,
                                                            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer,
                                                            Time time) {
        Serdes<K, V> serdes = new Serdes<>(name, keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
        return create(name, capacity, context, serdes, time);
    }


    protected static <K, V> InMemoryLRUCacheStore<K, V> create (String name, int capacity, ProcessorContext context,
                                                                Serdes<K, V> serdes, Time time) {
        MemoryLRUCache<K, V> cache = new MemoryLRUCache<K, V>(name, capacity);
        final InMemoryLRUCacheStore<K, V> store = new InMemoryLRUCacheStore<>(name, context, cache, serdes, time);
        cache.whenEldestRemoved(new EldestEntryRemovalListener<K, V>() {
            @Override
            public void apply(K key, V value) {
                store.removed(key);
            }
        });
        return store;

    }
    public InMemoryLRUCacheStore(String name, ProcessorContext context, MemoryLRUCache<K, V> cache, Serdes<K, V> serdes, Time time) {
        super(name, cache, context, serdes, "kafka-streams", time);
    }
    
    private static interface EldestEntryRemovalListener<K, V> {
        public void apply( K key, V value );
    }

    protected static final class MemoryLRUCache<K, V> implements KeyValueStore<K, V> {
        
        private final String name;
        private final Map<K, V> map;
        private final NavigableSet<K> keys;
        private EldestEntryRemovalListener<K, V> listener;

        public MemoryLRUCache(String name, final int maxCacheSize ) {
            this.name = name;
            this.keys = new TreeSet<>();
            // leave room for one extra entry to handle adding an entry before the oldest can be removed
            this.map = new LinkedHashMap<K,V>(maxCacheSize+1, 1.01f, true) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                    if (size() > maxCacheSize) {
                        K key = eldest.getKey();
                        keys.remove(key);
                        if ( listener != null ) listener.apply(key, eldest.getValue());
                        return true;
                    }
                    return false;
                }
            };
        }
        
        protected void whenEldestRemoved( EldestEntryRemovalListener<K, V> listener ) {
            this.listener = listener;
        }
        
        @Override
        public String name() {
            return this.name;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public V get(K key) {
            return this.map.get(key);
        }

        @Override
        public void put(K key, V value) {
            this.map.put(key, value);
            this.keys.add(key);
        }

        @Override
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public V delete(K key) {
            V value = this.map.remove(key);
            this.keys.remove(key);
            return value;
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryLRUCache.CacheIterator<K, V>(this.keys.subSet(from, true, to, false).iterator(), this.map);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryLRUCache.CacheIterator<K, V>(this.keys.iterator(), this.map);
        }

        @Override
        public void flush() {
            // do-nothing since it is in-memory
        }

        @Override
        public void close() {
            // do-nothing
        }

        private static class CacheIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<K> keys;
            private final Map<K, V> entries;
            private K lastKey;

            public CacheIterator(Iterator<K> keys, Map<K, V> entries) {
                this.keys = keys;
                this.entries = entries;
            }

            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                lastKey = keys.next();
                return new Entry<>(lastKey, entries.get(lastKey));
            }

            @Override
            public void remove() {
                keys.remove();
                entries.remove(lastKey);
            }

            @Override
            public void close() {
                // do nothing
            }
        }
    }
}
