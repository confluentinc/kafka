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
package org.apache.kafka.tiered.storage.utils;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.FileLogInputStream;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.LogFileUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class BrokerLocalStorage {

    private final Integer brokerId;
    private final Set<File> brokerStorageDirectories;
    private final Integer storageWaitTimeoutSec;

    private final int storagePollPeriodSec = 1;
    private final Time time = Time.SYSTEM;

    public BrokerLocalStorage(Integer brokerId,
                              Set<String> storageDirNames,
                              Integer storageWaitTimeoutSec) {
        this.brokerId = brokerId;
        this.brokerStorageDirectories = storageDirNames.stream().map(File::new).collect(Collectors.toSet());
        this.storageWaitTimeoutSec = storageWaitTimeoutSec;
    }

    public Integer getBrokerId() {
        return brokerId;
    }

    public Set<File> getBrokerStorageDirectories() {
        return brokerStorageDirectories;
    }

    /**
     * Wait until the first segment offset in Apache Kafka storage for the given topic-partition is
     * equal to the provided offset.
     * This ensures segments can be retrieved from the local tiered storage when expected.
     *
     * @param topicPartition The topic-partition to check.
     * @param offset The offset to wait for.
     * @throws AssertionError if the timeout is reached or the earliest-local offset is not equal to the provided
     *                        offset.
     */
    public void waitForEarliestLocalOffset(TopicPartition topicPartition,
                                           Long offset) {
        Function<OffsetHolder, Optional<String>> relativePosFunc = offsetHolder -> {
            Optional<String> result = Optional.empty();
            if (offsetHolder.firstLogFileBaseOffset < offset &&
                    !isOffsetPresentInFirstLocalSegment(topicPartition, offsetHolder.firstLogFileBaseOffset, offset)) {
                result = Optional.of("smaller than");
            } else if (offsetHolder.firstLogFileBaseOffset > offset) {
                result = Optional.of("ahead of");
            }
            return result;
        };
        waitForOffset(topicPartition, offset, relativePosFunc);
    }

    /**
     * Wait until the first segment offset in Apache Kafka storage for the given topic-partition is
     * equal or greater to the provided offset.
     * This ensures segments can be retrieved from the local tiered storage when expected.
     *
     * @param topicPartition The topic-partition to check.
     * @param offset The offset to wait for.
     * @throws AssertionError if the timeout is reached or the earliest-local offset is lesser than to the provided
     *                        offset.
     */
    public void waitForAtLeastEarliestLocalOffset(TopicPartition topicPartition,
                                                  Long offset) {
        Function<OffsetHolder, Optional<String>> relativePosFunc = offsetHolder -> {
            Optional<String> result = Optional.empty();
            if (offsetHolder.firstLogFileBaseOffset < offset &&
                    !isOffsetPresentInFirstLocalSegment(topicPartition, offsetHolder.firstLogFileBaseOffset, offset)) {
                result = Optional.of("smaller than");
            }
            return result;
        };
        waitForOffset(topicPartition, offset, relativePosFunc);
    }

    private void waitForOffset(TopicPartition topicPartition,
                               Long offset,
                               Function<OffsetHolder, Optional<String>> relativePosFunc) {
        Timer timer = time.timer(TimeUnit.SECONDS.toMillis(storageWaitTimeoutSec));
        OffsetHolder offsetHolder = new OffsetHolder(0L, List.of());
        while (timer.notExpired() && offsetHolder.firstLogFileBaseOffset < offset) {
            timer.sleep(TimeUnit.SECONDS.toMillis(storagePollPeriodSec));
            offsetHolder = getEarliestLocalOffset(topicPartition);
        }
        Optional<String> relativePos = relativePosFunc.apply(offsetHolder);
        if (relativePos.isPresent()) {
            String pos = relativePos.get();
            String message = String.format("[BrokerId=%d] The base offset of the first log segment of %s " +
                            "in the log directory is %d which is %s the expected offset %s. The directory of %s is " +
                            "made of the following files: %s", brokerId, topicPartition,
                    offsetHolder.firstLogFileBaseOffset, pos, offset, topicPartition,
                    String.join(System.lineSeparator(), offsetHolder.partitionFiles));
            throw new AssertionError(message);
        }
    }

    /**
     * Check if the given offset is present in the first local segment of the given topic-partition.
     * @param topicPartition The topic-partition to check.
     * @param firstLogFileBaseOffset The base offset of the first local segment.
     * @param offsetToSearch The offset to search.
     * @return true if the offset is present in the first local segment, false otherwise.
     */
    private boolean isOffsetPresentInFirstLocalSegment(TopicPartition topicPartition,
                                                       Long firstLogFileBaseOffset,
                                                       Long offsetToSearch)  {
        if (offsetToSearch < firstLogFileBaseOffset) {
            return false;
        }
        if (offsetToSearch.equals(firstLogFileBaseOffset)) {
            return true;
        }
        File logDir = brokerStorageDirectories.stream()
                .filter(dir -> dirContainsTopicPartition(topicPartition, dir))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("[BrokerId=%d] Directory for the topic-partition %s " +
                "was not found", brokerId, topicPartition)));
        File partitionDir = new File(logDir.getAbsolutePath(), topicPartition.toString());
        File firstSegmentFile = new File(partitionDir.getAbsolutePath(),
                LogFileUtils.filenamePrefixFromOffset(firstLogFileBaseOffset) + LogFileUtils.LOG_FILE_SUFFIX);
        try (FileRecords fileRecords = FileRecords.open(firstSegmentFile, false)) {
            for (FileLogInputStream.FileChannelRecordBatch batch : fileRecords.batches()) {
                if (batch.baseOffset() <= offsetToSearch && batch.lastOffset() >= offsetToSearch) {
                    return true;
                }
            }
        } catch (final IOException ex) {
            return false;
        }
        return false;
    }

    public void eraseStorage(FilenameFilter filter) throws IOException {
        for (File brokerDir : brokerStorageDirectories) {
            for (File file : Objects.requireNonNull(brokerDir.listFiles(filter))) {
                Utils.delete(file);
            }
        }
    }

    private OffsetHolder getEarliestLocalOffset(TopicPartition topicPartition) {
        List<String> partitionFiles = getTopicPartitionFileNames(topicPartition);
        Optional<String> firstLogFile = partitionFiles.stream()
                .filter(filename -> filename.endsWith(LogFileUtils.LOG_FILE_SUFFIX))
                .sorted()
                .findFirst();
        if (firstLogFile.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "[BrokerId=%d] No log file found for the topic-partition %s", brokerId, topicPartition));
        }
        return new OffsetHolder(LogFileUtils.offsetFromFileName(firstLogFile.get()), partitionFiles);
    }

    public boolean dirContainsTopicPartition(TopicPartition topicPartition, File logDir) {
        File[] files = getTopicPartitionFiles(topicPartition, Set.of(logDir));
        return files != null && files.length > 0;
    }

    private File[] getTopicPartitionFiles(TopicPartition topicPartition) {
        return getTopicPartitionFiles(topicPartition, brokerStorageDirectories);
    }

    private File[] getTopicPartitionFiles(TopicPartition topicPartition, Set<File> logDirs) {
        File[] files = null;
        for (File brokerDir : logDirs) {
            files = brokerDir.listFiles((dir, name) -> name.equals(topicPartition.toString()));
            // currently, we only expect one topic partition in one log dir
            if (files != null && files.length != 0) {
                break;
            }
        }
        return files;
    }

    private List<String> getTopicPartitionFileNames(TopicPartition topicPartition) {
        File[] files = getTopicPartitionFiles(topicPartition);
        if (files == null || files.length == 0) {
            throw new IllegalArgumentException(String.format("[BrokerId=%d] Directory for the topic-partition %s " +
                    "was not found", brokerId, topicPartition));
        }
        File topicPartitionDir = files[0];
        return Arrays.stream(Objects.requireNonNull(topicPartitionDir.listFiles()))
                .map(File::getName)
                .toList();
    }

    private record OffsetHolder(long firstLogFileBaseOffset, List<String> partitionFiles) {
    }
}
