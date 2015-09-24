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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public abstract class AbstractKeyValueStoreTest {

    protected static final File STATE_DIR = new File("build/data").getAbsoluteFile();

    protected abstract <K, V> KeyValueStore<K, V> createKeyValueStore(ProcessorContext context,
                                                                      Class<K> keyClass, Class<V> valueClass,
                                                                      boolean useContextSerdes);

    @After
    public void cleanup() {
        if (STATE_DIR.exists()) {
            try {
                Files.walkFileTree(STATE_DIR.toPath(), new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }

                });
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    @Test
    public void testIntegerKeysAndStringValues() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create();
        driver.useStateDir(STATE_DIR);
        KeyValueStore<Integer, String> store = createKeyValueStore(driver.context(), Integer.class, String.class, false);
        try {

            // Verify that the store reads and writes correctly ...
            store.put(0, "zero");
            store.put(1, "one");
            store.put(2, "two");
            store.put(4, "four");
            store.put(5, "five");
            assertEquals(5, driver.sizeOf(store));
            assertEquals("zero", store.get(0));
            assertEquals("one", store.get(1));
            assertEquals("two", store.get(2));
            assertNull(store.get(3));
            assertEquals("four", store.get(4));
            assertEquals("five", store.get(5));
            store.delete(5);

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();
            assertEquals("zero", driver.flushedEntryStored(0));
            assertEquals("one", driver.flushedEntryStored(1));
            assertEquals("two", driver.flushedEntryStored(2));
            assertEquals("four", driver.flushedEntryStored(4));
            assertEquals(null, driver.flushedEntryStored(5));

            assertEquals(false, driver.flushedEntryRemoved(0));
            assertEquals(false, driver.flushedEntryRemoved(1));
            assertEquals(false, driver.flushedEntryRemoved(2));
            assertEquals(false, driver.flushedEntryRemoved(4));
            assertEquals(true, driver.flushedEntryRemoved(5));
            
            // Check range iteration ...
            try ( KeyValueIterator<Integer,String> iter = store.range(2, 4)) {
                while ( iter.hasNext() ) {
                    Entry<Integer,String> entry = iter.next();
                    if ( entry.key().equals(2) ) assertEquals("two", entry.value());
                    else if ( entry.key().equals(4) ) assertEquals("four", entry.value());
                    else fail("Unexpected entry: " + entry);
                }
            }
            
            // Check range iteration ...
            try ( KeyValueIterator<Integer,String> iter = store.range(2, 6)) {
                while ( iter.hasNext() ) {
                    Entry<Integer,String> entry = iter.next();
                    if ( entry.key().equals(2) ) assertEquals("two", entry.value());
                    else if ( entry.key().equals(4) ) assertEquals("four", entry.value());
                    else fail("Unexpected entry: " + entry);
                }
            }
        } finally {
            store.close();
        }
    }

    @Test
    public void testIntegerKeysAndStringValuesUsingDefaultSerializersAndDeserializers() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        driver.useStateDir(STATE_DIR);
        KeyValueStore<Integer, String> store = createKeyValueStore(driver.context(), Integer.class, String.class, true);
        try {

            // Verify that the store reads and writes correctly ...
            store.put(0, "zero");
            store.put(1, "one");
            store.put(2, "two");
            store.put(4, "four");
            store.put(5, "five");
            assertEquals(5, driver.sizeOf(store));
            assertEquals("zero", store.get(0));
            assertEquals("one", store.get(1));
            assertEquals("two", store.get(2));
            assertNull(store.get(3));
            assertEquals("four", store.get(4));
            assertEquals("five", store.get(5));
            store.delete(5);

            // Flush the store and verify all current entries were properly flushed ...
            store.flush();
            assertEquals("zero", driver.flushedEntryStored(0));
            assertEquals("one", driver.flushedEntryStored(1));
            assertEquals("two", driver.flushedEntryStored(2));
            assertEquals("four", driver.flushedEntryStored(4));
            assertEquals(null, driver.flushedEntryStored(5));

            assertEquals(false, driver.flushedEntryRemoved(0));
            assertEquals(false, driver.flushedEntryRemoved(1));
            assertEquals(false, driver.flushedEntryRemoved(2));
            assertEquals(false, driver.flushedEntryRemoved(4));
            assertEquals(true, driver.flushedEntryRemoved(5));
        } finally {
            store.close();
        }
    }

    @Test
    public void testRestoringInetgerKeysAndValues() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        driver.useStateDir(STATE_DIR);

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addRestoreEntry(0, "zero");
        driver.addRestoreEntry(1, "one");
        driver.addRestoreEntry(2, "two");
        driver.addRestoreEntry(4, "four");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        KeyValueStore<Integer, String> store = createKeyValueStore(driver.context(), Integer.class, String.class, false);
        try {
            // Verify that the store's contents were properly restored ...
            assertEquals(0, driver.checkForRestoredEntries(store));

            // and there are no other entries ...
            assertEquals(4, driver.sizeOf(store));
        } finally {
            store.close();
        }
    }

    @Test
    public void testRestoringInetgerKeysAndValuesUsingDefaultSerializersAndDeserializers() {
        // Create the test driver ...
        KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        driver.useStateDir(STATE_DIR);

        // Add any entries that will be restored to any store
        // that uses the driver's context ...
        driver.addRestoreEntry(0, "zero");
        driver.addRestoreEntry(1, "one");
        driver.addRestoreEntry(2, "two");
        driver.addRestoreEntry(4, "four");

        // Create the store, which should register with the context and automatically
        // receive the restore entries ...
        KeyValueStore<Integer, String> store = createKeyValueStore(driver.context(), Integer.class, String.class, true);
        try {
            // Verify that the store's contents were properly restored ...
            assertEquals(0, driver.checkForRestoredEntries(store));

            // and there are no other entries ...
            assertEquals(4, driver.sizeOf(store));
        } finally {
            store.close();
        }
    }

}
