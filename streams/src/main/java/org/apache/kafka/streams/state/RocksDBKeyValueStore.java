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

package org.apache.kafka.streams.state;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link KeyValueStore} that stores all entries in a local RocksDB database.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class RocksDBKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> {

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage and the system time
     * provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @return the key-value store
     */
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context, Class<K> keyClass, Class<V> valueClass) {
        return new RocksDBKeyValueStore<>(name, context, Serdes.withBuiltinTypes(name, keyClass, valueClass),
                new SystemTime());
    }

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage and the given time
     * provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
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
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context, Class<K> keyClass, Class<V> valueClass,
                                                           Time time) {
        return new RocksDBKeyValueStore<>(name, context, Serdes.withBuiltinTypes(name, keyClass, valueClass),
                time);
    }

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage, the
     * {@link ProcessorContext}'s default serializers and deserializers, and the system time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @return the key-value store
     */
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context) {
        return create(name, context, new SystemTime());
    }

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage, the
     * {@link ProcessorContext}'s default serializers and deserializers, and the given time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param time the time provider; may not be null
     * @return the key-value store
     */
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context, Time time) {
        return new RocksDBKeyValueStore<>(name, context, new Serdes<K, V>(name, context), time);
    }

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage, the
     * supplied serializers and deserializers, and the system time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @return the key-value store
     */
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context,
                                                           Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                           Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        return create(name, context, keySerializer, keyDeserializer, valueSerializer, valueDeserializer, new SystemTime());
    }

    /**
     * Create a key value store that records changes to a Kafka topic and that uses RocksDB for local storage, the
     * supplied serializers and deserializers, and the given time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @param time the time provider; may not be null
     * @return the key-value store
     */
    public static <K, V> RocksDBKeyValueStore<K, V> create(String name, ProcessorContext context,
                                                           Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                           Serializer<V> valueSerializer, Deserializer<V> valueDeserializer,
                                                           Time time) {
        Serdes<K, V> serdes = new Serdes<>(name, keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
        return new RocksDBKeyValueStore<>(name, context, serdes, time);
    }

    protected RocksDBKeyValueStore(String name, ProcessorContext context, Serdes<K, V> serdes, Time time) {
        super(name, new RocksDBStore<K, V>(name, context, serdes), context, serdes, "kafka-streams", time);
    }

    private static class RocksDBStore<K, V> implements KeyValueStore<K, V> {

        private static final int TTL_NOT_USED = -1;

        // TODO: these values should be configurable
        private static final long WRITE_BUFFER_SIZE = 32 * 1024 * 1024L;
        private static final long BLOCK_CACHE_SIZE = 100 * 1024 * 1024L;
        private static final long BLOCK_SIZE = 4096L;
        private static final int TTL_SECONDS = TTL_NOT_USED;
        private static final int MAX_WRITE_BUFFERS = 3;
        private static final CompressionType COMPRESSION_TYPE = CompressionType.NO_COMPRESSION;
        private static final CompactionStyle COMPACTION_STYLE = CompactionStyle.UNIVERSAL;
        private static final String DB_FILE_DIR = "rocksdb";

        private final Serdes<K, V> serdes;

        private final String topic;
        private final int partition;
        private final ProcessorContext context;

        private final Options options;
        private final WriteOptions wOptions;
        private final FlushOptions fOptions;

        private final String dbName;
        private final String dirName;

        private RocksDB db;

        public RocksDBStore(String name, ProcessorContext context, Serdes<K, V> serdes) {
            this.topic = name;
            this.partition = context.id();
            this.context = context;
            this.serdes = serdes;

            // initialize the rocksdb options
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
            tableConfig.setBlockCacheSize(BLOCK_CACHE_SIZE);
            tableConfig.setBlockSize(BLOCK_SIZE);

            options = new Options();
            options.setTableFormatConfig(tableConfig);
            options.setWriteBufferSize(WRITE_BUFFER_SIZE);
            options.setCompressionType(COMPRESSION_TYPE);
            options.setCompactionStyle(COMPACTION_STYLE);
            options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            options.setCreateIfMissing(true);
            options.setErrorIfExists(false);

            wOptions = new WriteOptions();
            wOptions.setDisableWAL(true);

            fOptions = new FlushOptions();
            fOptions.setWaitForFlush(true);

            dbName = this.topic + "." + this.partition;
            dirName = this.context.stateDir() + File.separator + DB_FILE_DIR;

            db = openDB(new File(dirName, dbName), this.options, TTL_SECONDS);
        }

        private RocksDB openDB(File dir, Options options, int ttl) {
            try {
                if (ttl == TTL_NOT_USED) {
                    return RocksDB.open(options, dir.toString());
                } else {
                    throw new KafkaException("Change log is not supported for store " + this.topic + " since it is TTL based.");
                    // TODO: support TTL with change log?
                    // return TtlDB.open(options, dir.toString(), ttl, false);
                }
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error opening store " + this.topic + " at location " + dir.toString(), e);
            }
        }

        @Override
        public String name() {
            return this.topic;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public V get(K key) {
            try {
                return serdes.valueFrom(this.db.get(serdes.rawKey(key)));
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing get " + key.toString() + " from store " + this.topic, e);
            }
        }

        @Override
        public void put(K key, V value) {
            try {
                if (value == null) {
                    db.remove(wOptions, serdes.rawKey(key));
                } else {
                    db.put(wOptions, serdes.rawKey(key), serdes.rawValue(value));
                }
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing put " + key.toString() + " from store " + this.topic, e);
            }
        }

        @Override
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }
        
        @Override
        public V delete(K key) {
            V value = get(key);
            put(key, null);
            return value;
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new RocksDBRangeIterator<K, V>(db.newIterator(), serdes, from, to);
        }

        @Override
        public KeyValueIterator<K, V> all() {
            RocksIterator innerIter = db.newIterator();
            innerIter.seekToFirst();
            return new RocksDbIterator<K, V>(innerIter, serdes);
        }

        @Override
        public void flush() {
            try {
                db.flush(fOptions);
            } catch (RocksDBException e) {
                // TODO: this needs to be handled more accurately
                throw new KafkaException("Error while executing flush from store " + this.topic, e);
            }
        }

        @Override
        public void close() {
            flush();
            db.close();
        }

        private static class RocksDbIterator<K, V> implements KeyValueIterator<K, V> {
            private final RocksIterator iter;
            private final Serdes<K, V> serdes;

            public RocksDbIterator(RocksIterator iter, Serdes<K, V> serdes) {
                this.iter = iter;
                this.serdes = serdes;
            }

            protected byte[] peekRawKey() {
                return iter.key();
            }

            protected Entry<K, V> getEntry() {
                return new Entry<>(serdes.keyFrom(iter.key()), serdes.valueFrom(iter.value()));
            }

            @Override
            public boolean hasNext() {
                return iter.isValid();
            }

            @Override
            public Entry<K, V> next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                Entry<K, V> entry = this.getEntry();
                iter.next();
                return entry;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("RocksDB iterator does not support remove");
            }

            @Override
            public void close() {
            }

        }

        private static class LexicographicComparator implements Comparator<byte[]> {

            @Override
            public int compare(byte[] left, byte[] right) {
                for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
                    int leftByte = left[i] & 0xff;
                    int rightByte = right[j] & 0xff;
                    if (leftByte != rightByte) {
                        return leftByte - rightByte;
                    }
                }
                return left.length - right.length;
            }
        }

        private static class RocksDBRangeIterator<K, V> extends RocksDbIterator<K, V> {
            // RocksDB's JNI interface does not expose getters/setters that allow the
            // comparator to be pluggable, and the default is lexicographic, so it's
            // safe to just force lexicographic comparator here for now.
            private final Comparator<byte[]> comparator = new LexicographicComparator();
            byte[] to;

            public RocksDBRangeIterator(RocksIterator iter, Serdes<K, V> serdes,
                    K from, K to) {
                super(iter, serdes);
                iter.seek(serdes.rawKey(from));
                this.to = serdes.rawKey(to);
            }

            @Override
            public boolean hasNext() {
                return super.hasNext() && comparator.compare(super.peekRawKey(), this.to) < 0;
            }
        }

    }
}
