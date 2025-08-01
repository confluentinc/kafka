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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;

/**
 * A persistent key-(value-timestamp) store based on RocksDB.
 */
public class RocksDBTimestampedStore extends RocksDBStore implements TimestampedBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStore.class);

    private static final byte[] TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME = "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedStore(final String name,
                            final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBTimestampedStore(final String name,
                            final String parentDir,
                            final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
                dbOptions,
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );
        final ColumnFamilyHandle noTimestampColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle withTimestampColumnFamily = columnFamilies.get(1);

        final RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
        noTimestampsIter.seekToFirst();
        if (noTimestampsIter.isValid()) {
            log.info("Opening store {} in upgrade mode", name);
            cfAccessor = new DualColumnFamilyAccessor(noTimestampColumnFamily, withTimestampColumnFamily);
        } else {
            log.info("Opening store {} in regular mode", name);
            cfAccessor = new SingleColumnFamilyAccessor(withTimestampColumnFamily);
            noTimestampColumnFamily.close();
        }
        noTimestampsIter.close();
    }

    private class DualColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle oldColumnFamily;
        private final ColumnFamilyHandle newColumnFamily;

        private DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
                                         final ColumnFamilyHandle newColumnFamily) {
            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                        final byte[] key,
                        final byte[] valueWithTimestamp) {
            synchronized (position) {
                if (valueWithTimestamp == null) {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.delete(newColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                } else {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.put(newColumnFamily, key, valueWithTimestamp);
                        StoreQueryUtils.updatePosition(position, context);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                    }
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                 final WriteBatchInterface batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key, Optional.empty());
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
            return get(accessor, key, Optional.of(readOptions));
        }

        private byte[] get(final DBAccessor accessor, final byte[] key, final Optional<ReadOptions> readOptions) throws RocksDBException {
            final byte[] valueWithTimestamp = readOptions.isPresent() ? accessor.get(newColumnFamily, readOptions.get(), key) : accessor.get(newColumnFamily, key);
            if (valueWithTimestamp != null) {
                return valueWithTimestamp;
            }

            final byte[] plainValue = readOptions.isPresent() ? accessor.get(oldColumnFamily, readOptions.get(), key) : accessor.get(oldColumnFamily, key);
            if (plainValue != null) {
                final byte[] valueWithUnknownTimestamp = convertToTimestampedFormat(plainValue);
                // this does only work, because the changelog topic contains correct data already
                // for other format changes, we cannot take this short cut and can only migrate data
                // from old to new store on put()
                put(accessor, key, valueWithUnknownTimestamp);
                return valueWithUnknownTimestamp;
            }

            return null;
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            final byte[] valueWithTimestamp = accessor.get(newColumnFamily, key);
            if (valueWithTimestamp != null) {
                return valueWithTimestamp;
            }

            final byte[] plainValue = accessor.get(oldColumnFamily, key);
            if (plainValue != null) {
                return convertToTimestampedFormat(plainValue);
            }

            return null;
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                            final Bytes from,
                                                            final Bytes to,
                                                            final boolean forward) {
            return new RocksDBDualCFRangeIterator(
                name,
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                from,
                to,
                forward,
                true);
        }

        @Override
        public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
            try {
                accessor.deleteRange(oldColumnFamily, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
            try {
                accessor.deleteRange(newColumnFamily, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward) {
            final RocksIterator innerIterWithTimestamp = accessor.newIterator(newColumnFamily);
            final RocksIterator innerIterNoTimestamp = accessor.newIterator(oldColumnFamily);
            if (forward) {
                innerIterWithTimestamp.seekToFirst();
                innerIterNoTimestamp.seekToFirst();
            } else {
                innerIterWithTimestamp.seekToLast();
                innerIterNoTimestamp.seekToLast();
            }
            return new RocksDBDualCFIterator(name, innerIterWithTimestamp, innerIterNoTimestamp, forward);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix) {
            final Bytes to = incrementWithoutOverflow(prefix);
            return new RocksDBDualCFRangeIterator(
                name,
                accessor.newIterator(newColumnFamily),
                accessor.newIterator(oldColumnFamily),
                prefix,
                to,
                true,
                false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
            return accessor.approximateNumEntries(oldColumnFamily) +
                    accessor.approximateNumEntries(newColumnFamily);
        }

        @Override
        public void flush(final DBAccessor accessor) throws RocksDBException {
            accessor.flush(oldColumnFamily, newColumnFamily);
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatchInterface batch) throws RocksDBException {
            if (value == null) {
                batch.delete(oldColumnFamily, key);
                batch.delete(newColumnFamily, key);
            } else {
                batch.delete(oldColumnFamily, key);
                batch.put(newColumnFamily, key, value);
            }
        }

        @Override
        public void close() {
            oldColumnFamily.close();
            newColumnFamily.close();
        }
    }

    private static class RocksDBDualCFIterator extends AbstractIterator<KeyValue<Bytes, byte[]>>
        implements ManagedKeyValueIterator<Bytes, byte[]> {

        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;

        private final String storeName;
        private final RocksIterator iterWithTimestamp;
        private final RocksIterator iterNoTimestamp;
        private final boolean forward;

        private volatile boolean open = true;

        private byte[] nextWithTimestamp;
        private byte[] nextNoTimestamp;
        private KeyValue<Bytes, byte[]> next;
        private Runnable closeCallback = null;

        RocksDBDualCFIterator(final String storeName,
                              final RocksIterator iterWithTimestamp,
                              final RocksIterator iterNoTimestamp,
                              final boolean forward) {
            this.iterWithTimestamp = iterWithTimestamp;
            this.iterNoTimestamp = iterNoTimestamp;
            this.storeName = storeName;
            this.forward = forward;
        }

        @Override
        public synchronized boolean hasNext() {
            if (!open) {
                throw new InvalidStateStoreException(String.format("RocksDB iterator for store %s has closed", storeName));
            }
            return super.hasNext();
        }

        @Override
        public synchronized KeyValue<Bytes, byte[]> next() {
            return super.next();
        }

        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            if (nextNoTimestamp == null && iterNoTimestamp.isValid()) {
                nextNoTimestamp = iterNoTimestamp.key();
            }

            if (nextWithTimestamp == null && iterWithTimestamp.isValid()) {
                nextWithTimestamp = iterWithTimestamp.key();
            }

            if (nextNoTimestamp == null && !iterNoTimestamp.isValid()) {
                if (nextWithTimestamp == null && !iterWithTimestamp.isValid()) {
                    return allDone();
                } else {
                    next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                    nextWithTimestamp = null;
                    if (forward) {
                        iterWithTimestamp.next();
                    } else {
                        iterWithTimestamp.prev();
                    }
                }
            } else {
                if (nextWithTimestamp == null) {
                    next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                    nextNoTimestamp = null;
                    if (forward) {
                        iterNoTimestamp.next();
                    } else {
                        iterNoTimestamp.prev();
                    }
                } else {
                    if (forward) {
                        if (comparator.compare(nextNoTimestamp, nextWithTimestamp) <= 0) {
                            next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                            nextNoTimestamp = null;
                            iterNoTimestamp.next();
                        } else {
                            next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                            nextWithTimestamp = null;
                            iterWithTimestamp.next();
                        }
                    } else {
                        if (comparator.compare(nextNoTimestamp, nextWithTimestamp) >= 0) {
                            next = KeyValue.pair(new Bytes(nextNoTimestamp), convertToTimestampedFormat(iterNoTimestamp.value()));
                            nextNoTimestamp = null;
                            iterNoTimestamp.prev();
                        } else {
                            next = KeyValue.pair(new Bytes(nextWithTimestamp), iterWithTimestamp.value());
                            nextWithTimestamp = null;
                            iterWithTimestamp.prev();
                        }
                    }
                }
            }
            return next;
        }

        @Override
        public synchronized void close() {
            if (closeCallback == null) {
                throw new IllegalStateException("RocksDBDualCFIterator expects close callback to be set immediately upon creation");
            }
            closeCallback.run();

            iterNoTimestamp.close();
            iterWithTimestamp.close();
            open = false;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.key;
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }
    }

    private static class RocksDBDualCFRangeIterator extends RocksDBDualCFIterator {
        // RocksDB's JNI interface does not expose getters/setters that allow the
        // comparator to be pluggable, and the default is lexicographic, so it's
        // safe to just force lexicographic comparator here for now.
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private final byte[] rawLastKey;
        private final boolean forward;
        private final boolean toInclusive;

        RocksDBDualCFRangeIterator(final String storeName,
                                   final RocksIterator iterWithTimestamp,
                                   final RocksIterator iterNoTimestamp,
                                   final Bytes from,
                                   final Bytes to,
                                   final boolean forward,
                                   final boolean toInclusive) {
            super(storeName, iterWithTimestamp, iterNoTimestamp, forward);
            this.forward = forward;
            this.toInclusive = toInclusive;
            if (forward) {
                if (from == null) {
                    iterWithTimestamp.seekToFirst();
                    iterNoTimestamp.seekToFirst();
                } else {
                    iterWithTimestamp.seek(from.get());
                    iterNoTimestamp.seek(from.get());
                }
                rawLastKey = to == null ? null : to.get();
            } else {
                if (to == null) {
                    iterWithTimestamp.seekToLast();
                    iterNoTimestamp.seekToLast();
                } else {
                    iterWithTimestamp.seekForPrev(to.get());
                    iterNoTimestamp.seekForPrev(to.get());
                }
                rawLastKey = from == null ? null : from.get();
            }
        }

        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            final KeyValue<Bytes, byte[]> next = super.makeNext();

            if (next == null) {
                return allDone();
            } else if (rawLastKey == null) {
                //null means range endpoint is open
                return next;
            } else {
                if (forward) {
                    if (comparator.compare(next.key.get(), rawLastKey) < 0) {
                        return next;
                    } else if (comparator.compare(next.key.get(), rawLastKey) == 0) {
                        return toInclusive ? next : allDone();
                    } else {
                        return allDone();
                    }
                } else {
                    if (comparator.compare(next.key.get(), rawLastKey) >= 0) {
                        return next;
                    } else {
                        return allDone();
                    }
                }
            }
        }
    }
}
