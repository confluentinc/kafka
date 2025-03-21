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
package org.apache.kafka.common.compress;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * A thread-safe pool of byte arrays for use by {@link Lz4BlockOutputStream}.
 */
public class Lz4ArrayPool {
    public static final Lz4ArrayPool INSTANCE = new Lz4ArrayPool();

    /**
     * Buckets for powers of two, corresponding to LZ4 block sizes. We have to support both
     * uncompressed and maximum compressed block sizes, but since the maximum compressed block
     * sizes are only 0.5% larger than the uncompressed block sizes, we can use the same
     * pool for both and always hand out buffers of the maximum compressed block size.
     *
     * Since there are only 4 valid LZ4 block sizes, only 4 buckets in the list are non-null.
     *
     * To allow the garbage collector to reclaim memory when under memory pressure, we hold
     * SoftReferences to the byte arrays in the pool.
     */
    private List<Queue<SoftReference<byte[]>>> buckets = new ArrayList<>(32);
    private int[] bucketCapacities = new int[32];

    public Lz4ArrayPool() {
        // Initialize all buckets.
        for (int i = 0; i < 32; i++) {
            buckets.add(null);
        }

        // Initialize buckets for the range of valid block sizes.
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        for (int blockSizeValue = Lz4BlockOutputStream.BD.MINIMUM_BLOCK_SIZE_VALUE; blockSizeValue <= Lz4BlockOutputStream.BD.MAXIMUM_BLOCK_SIZE_VALUE; blockSizeValue++) {
            int blockSize = new Lz4BlockOutputStream.BD(blockSizeValue).getBlockMaximumSize();
            int bucket = bucket(blockSize);
            buckets.set(bucket, new ConcurrentLinkedQueue<>());
            bucketCapacities[bucket] = compressor.maxCompressedLength(blockSize);
        }
    }

    /**
     * Gets a byte array from the pool with at least the given capacity.
     * @param capacity The minimum capacity of the byte array.
     * @return A byte array with at least the given capacity.
     *
     * @throws IllegalArgumentException If the capacity is not a valid compressed or uncompressed
     *                                  LZ4 block size.
     */
    public byte[] get(int capacity) {
        int bucket = bucket(capacity);
        Queue<SoftReference<byte[]>> bucketQueue = buckets.get(bucket);
        int bucketCapacity = bucketCapacities[bucket];

        // Validate capacity.
        if (bucketQueue == null || capacity > bucketCapacity) {
            throw new IllegalArgumentException("Capacity " + capacity + " is not a valid LZ4 block size");
        }

        // Find an array that hasn't been reclaimed by the garbage collector.
        SoftReference<byte[]> reference;
        while ((reference = bucketQueue.poll()) != null) {
            byte[] buffer = reference.get();
            if (buffer != null) {
                return buffer;
            }
        }

        // Otherwise allocate a new array.
        return new byte[bucketCapacity];
    }

    /**
     * Returns a byte array to the pool.
     * @param buffer The byte array to return to the pool.
     *
     * @throws IllegalArgumentException If the byte array was not allocated by the pool.
     */
    public void release(byte[] buffer) {
        int bucket = bucket(buffer.length);
        Queue<SoftReference<byte[]>> bucketQueue = buckets.get(bucket);
        int bucketCapacity = bucketCapacities[bucket];

        // Validate capacity.
        if (bucketQueue == null || buffer.length != bucketCapacity) {
            throw new IllegalArgumentException("Cannot release buffer of size " + buffer.length + " that was not allocated by the pool");
        }

        Arrays.fill(buffer, (byte) 0);
        bucketQueue.add(new SoftReference<byte[]>(buffer));
    }

    /**
     * Returns the bucket index for the given capacity.
     * Equivalent to floor(log2(capacity)) for values greater than 0.
     *
     * @param capacity The capacity.
     * @return The bucket index.
     */
    private int bucket(int capacity) {
        if (capacity == 0) {
            return 0;
        }
        return 31 - Integer.numberOfLeadingZeros(capacity);
    }
}
