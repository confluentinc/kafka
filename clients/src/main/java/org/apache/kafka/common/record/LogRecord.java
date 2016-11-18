/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import java.nio.ByteBuffer;

public interface LogRecord {

    long offset();

    long sequence();

    long nextOffset();

    int sizeInBytes();

    byte attributes();

    long timestamp();

    long checksum();

    boolean isValid();

    void ensureValid();

    int keySize();

    boolean hasKey();

    ByteBuffer key();

    int valueSize();

    boolean hasNullValue();

    ByteBuffer value();


    /**
     * For versions prior to 2, the record contains its own magic.
     * @param magic
     * @return
     */
    boolean hasMagic(byte magic);

    /**
     * For versions prior to 2
     * @return
     */
    boolean isCompressed();

    /**
     * For versions prior to 2
     * @param timestampType
     * @return
     */
    boolean hasTimestampType(TimestampType timestampType);

}
