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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new entries, possibly with message
 * format conversion.
 */
public class MemoryRecordsBuilder {
    private static final float COMPRESSION_RATE_DAMPING_FACTOR = 0.9f;
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    private static final int COMPRESSION_DEFAULT_BUFFER_SIZE = 1024;

    private static final float[] TYPE_TO_RATE;

    static {
        int maxTypeId = -1;
        for (CompressionType type : CompressionType.values())
            maxTypeId = Math.max(maxTypeId, type.id);
        TYPE_TO_RATE = new float[maxTypeId + 1];
        for (CompressionType type : CompressionType.values()) {
            TYPE_TO_RATE[type.id] = type.rate;
        }
    }

    private final TimestampType timestampType;
    private final CompressionType compressionType;
    private final DataOutputStream appendStream;
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initPos;
    private final long baseOffset;
    private final long logAppendTime;
    private final long pid;
    private final short epoch;
    private final int baseSequence;
    private final boolean isTransactional;
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int initialCapacity;

    private long writtenUncompressed = 0;
    private long numRecords = 0;
    private float compressionRate = 1;
    private long maxTimestamp = LogEntry.NO_TIMESTAMP;
    private long offsetOfMaxTimestamp = -1;
    private long lastOffset = -1;
    private Long baseTimestamp = null;

    private MemoryRecords builtRecords;

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compressionType The compression codec to use
     * @param timestampType The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset The initial offset to use for
     * @param logAppendTime The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param pid The producer ID associated with the producer writing this record set
     * @param epoch The epoch of the pid
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long pid,
                                short epoch,
                                int baseSequence,
                                boolean isTransactional,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        if (magic > LogEntry.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");

        if (isTransactional) {
            if (pid == LogEntry.NO_PID)
                throw new IllegalArgumentException("Cannot write transactional messages without a valid PID");

            if (magic < LogEntry.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Transactional messages are not supported for magic " + magic);
        }

        if (pid != LogEntry.NO_PID) {
            if (epoch  < 0)
                throw new IllegalArgumentException("Invalid negative epoch number");

            if (baseSequence < 0)
                 throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < LogEntry.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }

        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.initPos = buffer.position();
        this.numRecords = 0;
        this.writtenUncompressed = 0;
        this.compressionRate = 1;
        this.maxTimestamp = LogEntry.NO_TIMESTAMP;
        this.pid = pid;
        this.epoch = epoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialCapacity = buffer.capacity();

        if (magic > LogEntry.MAGIC_VALUE_V1) {
            buffer.position(initPos + EosLogEntry.RECORDS_OFFSET);
        } else if (compressionType != CompressionType.NONE) {
            // for compressed records, leave space for the header and the shallow message metadata
            // and move the starting position to the value payload offset
            buffer.position(initPos + Records.LOG_OVERHEAD + Record.recordOverhead(magic));
        }

        // create the stream
        bufferStream = new ByteBufferOutputStream(buffer);
        appendStream = new DataOutputStream(compressionType.wrapForOutput(bufferStream, magic,
                COMPRESSION_DEFAULT_BUFFER_SIZE));
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return initialCapacity;
    }

    public double compressionRate() {
        return compressionRate;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
        close();
        return builtRecords;
    }

    /**
     * Get the max timestamp and its offset. If the log append time is used, then the offset will
     * be either the first offset in the set if no compression is used or the last offset otherwise.
     * @return The max timestamp and its offset
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            return new RecordsInfo(logAppendTime,  lastOffset);
        else if (maxTimestamp == LogEntry.NO_TIMESTAMP)
            return new RecordsInfo(LogEntry.NO_TIMESTAMP, lastOffset);
        else
            return new RecordsInfo(maxTimestamp, compressionType == CompressionType.NONE ? offsetOfMaxTimestamp : lastOffset);
    }

    public void close() {
        if (builtRecords != null)
            return;

        try {
            appendStream.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        if (numRecords == 0L) {
            buffer().position(initPos);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > 1)
                writeEosLogEntryHeader();
            else if (compressionType != CompressionType.NONE)
                writeOldEntryCompressedWrapperHeader();

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initPos);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void writeEosLogEntryHeader() {
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);
        int size = pos - initPos;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long baseTimestamp;
        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            baseTimestamp = logAppendTime;
            maxTimestamp = logAppendTime;
        } else {
            baseTimestamp = this.baseTimestamp;
            maxTimestamp = this.maxTimestamp;
        }

        EosLogEntry.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                baseTimestamp, maxTimestamp, pid, epoch, baseSequence, isTransactional, partitionLeaderEpoch);

        buffer.position(pos);
    }

    private void writeOldEntryCompressedWrapperHeader() {
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initPos);

        int wrapperSize = pos - initPos - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - Record.recordOverhead(magic);
        OldLogEntry.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        Record.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);

        // update the compression ratio
        this.compressionRate = (float) writtenCompressed / this.writtenUncompressed;
        TYPE_TO_RATE[compressionType.id] = TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_DAMPING_FACTOR +
            compressionRate * (1 - COMPRESSION_RATE_DAMPING_FACTOR);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key, ByteBuffer value) {
        try {
            if (lastOffset >= 0 && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != LogEntry.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (isControlRecord && magic < LogEntry.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Magic v" + magic + " does not support control records");

            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            if (magic > LogEntry.MAGIC_VALUE_V1)
                return appendEosLogRecord(offset, isControlRecord, timestamp, key, value);
            else
                return appendOldLogRecord(offset, timestamp, key, value);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value));
    }

    private long appendEosLogRecord(long offset, boolean isControlRecord, long timestamp,
                                    ByteBuffer key, ByteBuffer value) throws IOException {
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - baseTimestamp;
        long crc = EosLogRecord.writeTo(appendStream, isControlRecord, offsetDelta, timestampDelta, key, value);
        recordWritten(offset, timestamp, EosLogRecord.sizeInBytes(offsetDelta, timestamp, key, value));
        return crc;
    }

    private long appendOldLogRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value) throws IOException {
        if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;

        int size = Record.recordSize(magic, key, value);
        OldLogEntry.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        long crc = Record.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    public long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(nextSequentialOffset(), false, timestamp, key, value);
    }

    private long nextSequentialOffset() {
        return lastOffset < 0 ? baseOffset : lastOffset + 1;
    }

    /**
     * Append a new record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return crc of the record
     */
    public long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value));
    }

    public long append(KafkaRecord record) {
        return append(record.timestamp(), record.key(), record.value());
    }

    public long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value);
    }

    public long appendWithOffset(long offset, KafkaRecord record) {
        return appendWithOffset(offset, false, record.timestamp(), record.key(), record.value());
    }

    /**
     * Add a record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, Record record) {
        try {
            int size = record.sizeInBytes();
            OldLogEntry.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     *
     * @param record
     */
    public void append(LogRecord record) {
        appendWithOffset(record.offset(), record.isControlRecord(), record.timestamp(), record.key(), record.value());
    }

    /**
     * Append a log record using a different offset
     * @param offset
     * @param record
     */
    public void appendWithOffset(long offset, LogRecord record) {
        appendWithOffset(offset, record.isControlRecord(), record.timestamp(), record.key(), record.value());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended entry.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        if (record.magic() != magic)
            throw new IllegalArgumentException("Inner log entries must have matching magic values as the wrapper");
        if (lastOffset >= 0 && offset <= lastOffset)
            throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s (Offsets must increase monotonically).", offset, lastOffset));
        appendUncheckedWithOffset(offset, record);
    }

    /**
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param record The record to add
     */
    public void append(Record record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compressionType != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        numRecords += 1;
        writtenUncompressed += size;
        lastOffset = offset;

        if (magic > LogEntry.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compressionType == CompressionType.NONE) {
            return buffer().position();
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return (int) (writtenUncompressed * TYPE_TO_RATE[compressionType.id] * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Check if we have room for a new record containing the given key/value pair
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is really used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     *
     * There is an exceptional case when appending a single message whose size is larger than the batch size, the
     * capacity will be the message size which is larger than the write limit, i.e. the batch size. In this case
     * the checking should be based on the capacity of the initialized buffer rather than the write limit in order
     * to accept this single record.
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value) {
        if (isFull())
            return false;

        final int recordSize;
        if (magic < LogEntry.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + Record.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = (int) (lastOffset + 1 - baseOffset);
            recordSize = EosLogRecord.sizeInBytes(nextOffsetDelta, timestamp, key, value);
        }

        return numRecords == 0 ?
                this.initialCapacity >= recordSize :
                this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return isClosed() || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    public int sizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }
}
