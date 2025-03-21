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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class Lz4ArrayPoolTest {

    @Test
    public void testBlockSizes() {
        Lz4ArrayPool arrayPool = Lz4ArrayPool.INSTANCE;
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        for (int blockSizeValue = Lz4BlockOutputStream.BD.MINIMUM_BLOCK_SIZE_VALUE; blockSizeValue <= Lz4BlockOutputStream.BD.MAXIMUM_BLOCK_SIZE_VALUE; blockSizeValue++) {
            int blockSize = new Lz4BlockOutputStream.BD(blockSizeValue).getBlockMaximumSize();
            int compressedBlockSize = compressor.maxCompressedLength(blockSize);

            byte[] buffer = arrayPool.get(blockSize);
            byte[] compressedBuffer = arrayPool.get(compressedBlockSize);
            assertNotSame(buffer, compressedBuffer);

            // Both buffers must be the compressed block size.
            assertEquals(compressedBlockSize, buffer.length);
            assertEquals(compressedBlockSize, compressedBuffer.length);

            arrayPool.release(buffer);
            arrayPool.release(compressedBuffer);
        }
    }

    @Test
    public void testInvalidBlockSizes() {
        Lz4ArrayPool arrayPool = Lz4ArrayPool.INSTANCE;
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        assertThrows(IllegalArgumentException.class, () -> arrayPool.get(Integer.MIN_VALUE));
        assertThrows(IllegalArgumentException.class, () -> arrayPool.get(Integer.MAX_VALUE));

        assertThrows(IllegalArgumentException.class, () -> arrayPool.get(-1));

        assertThrows(IllegalArgumentException.class, () -> arrayPool.get(0));
        assertThrows(IllegalArgumentException.class, () -> arrayPool.release(new byte[0]));

        for (int blockSizeValue = Lz4BlockOutputStream.BD.MINIMUM_BLOCK_SIZE_VALUE; blockSizeValue <= Lz4BlockOutputStream.BD.MAXIMUM_BLOCK_SIZE_VALUE; blockSizeValue++) {
            int blockSize = new Lz4BlockOutputStream.BD(blockSizeValue).getBlockMaximumSize();
            int compressedBlockSize = compressor.maxCompressedLength(blockSize);

            // Cannot allocate or release buffers just smaller than the compressed block size.
            assertThrows(IllegalArgumentException.class, () -> arrayPool.get(blockSize - 1));
            assertThrows(IllegalArgumentException.class, () -> arrayPool.release(new byte[blockSize - 1]));

            // Cannot allocate or release buffers just larger than the compressed block size.
            assertThrows(IllegalArgumentException.class, () -> arrayPool.get(compressedBlockSize + 1));
            assertThrows(IllegalArgumentException.class, () -> arrayPool.release(new byte[compressedBlockSize + 1]));

            // All released buffers must match the compressed block size.
            assertThrows(IllegalArgumentException.class, () -> arrayPool.release(new byte[blockSize]));
        }
    }

    @Test
    public void testRecycling() {
        Lz4ArrayPool arrayPool = Lz4ArrayPool.INSTANCE;
        int blockSize = new Lz4BlockOutputStream.BD(Lz4BlockOutputStream.BD.MINIMUM_BLOCK_SIZE_VALUE).getBlockMaximumSize();

        byte[] buffer1 = arrayPool.get(blockSize);
        byte[] buffer2 = arrayPool.get(blockSize);
        assertNotSame(buffer1, buffer2);

        // Release buffer2.
        arrayPool.release(buffer2);

        // buffer3 should be the same as buffer2.
        byte[] buffer3 = arrayPool.get(blockSize);
        assertSame(buffer2, buffer3);
    }
}
