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
package org.apache.kafka.clients.producer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;


/**
 * The interface for the {@link KafkaProducer}
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer<K, V> extends Closeable {

    /**
     * Needs to be called before any of the other transaction methods. Assumes that
     * the producer.app.id is specified in the producer configuration.
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer
     *      are committed or rolled back.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *
     * @throws IllegalStateException if the appId for the producer is not set
     *         in the configuration.
     */
    void initTransactions();

    /**
     * Should be called before the start of each new transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     *         transaction.app.id is active.
     */
    void beginTransaction();

    /**
     * Sends a list of consumed offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * consumed only if the transaction is committed successfully.
     *
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern.
     *
     * @throws ProducerFencedException if another producer is with the same
     *         transaction.app.id is active.
     */
    void sendOffsets(Map<TopicPartition, OffsetAndMetadata> offsets,
                     String consumerGroupId);

    /**
     * Commits the ongoing transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     *         transaction.app.id is active.
     */
    void commitTransaction();

    /**
     * Aborts the ongoing transaction.
     *
     * @throws ProducerFencedException if another producer is with the same
     *         transaction.app.id is active.
     */
    void abortTransaction();


    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * 
     * @param record The record to send
     * @return A future which will eventually contain the response information
     *
     * @throws UnrecognizedMessageException if the broker detects data loss: ie. previous messages which we expect
     *         to be committed are detected to be missing. This is a fatal error.
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     *
     * @throws UnrecognizedMessageException if the broker detects data loss: ie. previous messages which we expect
     *         to be committed are detected to be missing. This is a fatal error.
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
    
    /**
     * Flush any accumulated records from the producer. Blocks until all sends are complete.
     */
    public void flush();

    /**
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * Return a map of metrics maintained by the producer
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * Close this producer
     */
    public void close();

    /**
     * Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
     * timeout, fail any pending send requests and force close the producer.
     */
    public void close(long timeout, TimeUnit unit);

}
