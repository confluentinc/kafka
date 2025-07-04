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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;

import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode.EXACTLY_ONCE_V2;

/**
 * {@code StreamsProducer} manages the producers within a Kafka Streams application.
 * <p>
 * If EOS is enabled, it is responsible to init and begin transactions if necessary.
 * It also tracks the transaction status, ie, if a transaction is in-fight.
 * <p>
 * For non-EOS, the user should not call transaction related methods.
 */
public class StreamsProducer {
    private final Logger log;
    private final String logPrefix;

    private final ProcessingMode processingMode;
    private final Time time;

    private Producer<byte[], byte[]> producer;
    private boolean transactionInFlight = false;
    private boolean transactionInitialized = false;
    private double oldProducerTotalBlockedTime = 0;
    // we have a single `StreamsProducer` per thread, and thus a single `sendException` instance,
    // which we share across all tasks, ie, all `RecordCollectorImpl`
    private final AtomicReference<KafkaException> sendException = new AtomicReference<>(null);

    public StreamsProducer(final Producer<byte[], byte[]> producer,
                           final ProcessingMode processingMode,
                           final Time time,
                           final LogContext logContext) {
        this.producer = Objects.requireNonNull(producer, "producer cannot be null");
        this.processingMode = Objects.requireNonNull(processingMode, "processingMode cannot be null");
        this.time = Objects.requireNonNull(time, "time cannot be null");
        log = Objects.requireNonNull(logContext, "logContext cannot be null").logger(getClass());
        logPrefix = logContext.logPrefix().trim();
    }

    private String formatException(final String message) {
        return message + " [" + logPrefix + "]";
    }

    boolean eosEnabled() {
        return processingMode == EXACTLY_ONCE_V2;
    }

    boolean transactionInFlight() {
        return transactionInFlight;
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    void initTransaction() {
        if (!eosEnabled()) {
            throw new IllegalStateException(formatException("Exactly-once is not enabled"));
        }
        if (!transactionInitialized) {
            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            try {
                producer.initTransactions();
                transactionInitialized = true;
            } catch (final TimeoutException timeoutException) {
                log.warn(
                    "Timeout exception caught trying to initialize transactions. " +
                        "The broker is either slow or in bad state (like not having enough replicas) in " +
                        "responding to the request, or the connection to broker was interrupted sending " +
                        "the request or receiving the response. " +
                        "Will retry initializing the task in the next loop. " +
                        "Consider overwriting {} to a larger value to avoid timeout errors",
                    ProducerConfig.MAX_BLOCK_MS_CONFIG
                );

                // re-throw to trigger `task.timeout.ms`
                throw timeoutException;
            } catch (final KafkaException exception) {
                throw new StreamsException(
                    formatException("Error encountered trying to initialize transactions"),
                    exception
                );
            }
        }
    }

    public void resetProducer(final Producer<byte[], byte[]> producer) {
        if (!eosEnabled()) {
            throw new IllegalStateException("Expected EOS to be enabled, but processing mode is " + processingMode);
        }

        oldProducerTotalBlockedTime += totalBlockedTime(this.producer);
        final long start = time.nanoseconds();
        close();
        final long closeTime = time.nanoseconds() - start;
        oldProducerTotalBlockedTime += closeTime;

        this.producer = producer;
    }

    private double getMetricValue(final Map<MetricName, ? extends Metric> metrics,
                                  final String name) {
        final List<MetricName> found = metrics.keySet().stream()
            .filter(n -> n.name().equals(name))
            .collect(Collectors.toList());
        if (found.isEmpty()) {
            return 0.0;
        }
        if (found.size() > 1) {
            final String err = String.format(
                "found %d values for metric %s. total blocked time computation may be incorrect",
                found.size(),
                name
            );
            log.error(err);
            throw new IllegalStateException(err);
        }
        return (Double) metrics.get(found.get(0)).metricValue();
    }

    private double totalBlockedTime(final Producer<?, ?> producer) {
        return getMetricValue(producer.metrics(), "bufferpool-wait-time-ns-total")
            + getMetricValue(producer.metrics(), "flush-time-ns-total")
            + getMetricValue(producer.metrics(), "txn-init-time-ns-total")
            + getMetricValue(producer.metrics(), "txn-begin-time-ns-total")
            + getMetricValue(producer.metrics(), "txn-send-offsets-time-ns-total")
            + getMetricValue(producer.metrics(), "txn-commit-time-ns-total")
            + getMetricValue(producer.metrics(), "txn-abort-time-ns-total")
            + getMetricValue(producer.metrics(), "metadata-wait-time-ns-total");
    }

    public double totalBlockedTime() {
        return oldProducerTotalBlockedTime + totalBlockedTime(producer);
    }

    private void maybeBeginTransaction() {
        if (eosEnabled() && !transactionInFlight) {
            try {
                producer.beginTransaction();
                transactionInFlight = true;
            } catch (final ProducerFencedException | InvalidProducerEpochException | InvalidPidMappingException error) {
                throw new TaskMigratedException(
                    formatException("Producer got fenced trying to begin a new transaction"),
                    error
                );
            } catch (final KafkaException error) {
                throw new StreamsException(
                    formatException("Error encountered trying to begin a new transaction"),
                    error
                );
            }
        }
    }

    AtomicReference<KafkaException> sendException() {
        return sendException;
    }

    Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record,
                                final Callback callback) {
        maybeBeginTransaction();
        try {
            return producer.send(record, callback);
        } catch (final KafkaException uncaughtException) {
            if (isRecoverable(uncaughtException)) {
                // producer.send() call may throw a KafkaException which wraps a FencedException,
                // in this case we should throw its wrapped inner cause so that it can be
                // captured and re-wrapped as TaskMigratedException
                throw new TaskMigratedException(
                    formatException("Producer got fenced trying to send a record"),
                    uncaughtException.getCause()
                );
            } else {
                throw new StreamsException(
                    formatException(String.format("Error encountered trying to send record to topic %s", record.topic())),
                    uncaughtException
                );
            }
        }
    }

    private static boolean isRecoverable(final KafkaException uncaughtException) {
        return uncaughtException.getCause() instanceof ProducerFencedException ||
            uncaughtException.getCause() instanceof InvalidPidMappingException ||
            uncaughtException.getCause() instanceof InvalidProducerEpochException ||
            uncaughtException.getCause() instanceof UnknownProducerIdException;
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     * @throws TaskMigratedException
     */
    public void commitTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                     final ConsumerGroupMetadata consumerGroupMetadata) {
        if (!eosEnabled()) {
            throw new IllegalStateException(formatException("Exactly-once is not enabled"));
        }
        maybeBeginTransaction();
        try {
            producer.sendOffsetsToTransaction(offsets, consumerGroupMetadata);
        } catch (final ProducerFencedException | InvalidProducerEpochException | CommitFailedException | InvalidPidMappingException error) {
            throw new TaskMigratedException(
                formatException("Producer got fenced trying to add offsets to a transaction"),
                error
            );
        } catch (final TimeoutException timeoutException) {
            // re-throw to trigger `task.timeout.ms`
            throw timeoutException;
        } catch (final KafkaException error) {
            throw new StreamsException(
                formatException("Error encountered trying to add offsets to a transaction"),
                error
            );
        }

        try {
            producer.commitTransaction();
            transactionInFlight = false;
        } catch (final ProducerFencedException | InvalidProducerEpochException | CommitFailedException | InvalidPidMappingException error) {
            throw new TaskMigratedException(
                formatException("Producer got fenced trying to commit a transaction"),
                error
            );
        } catch (final TimeoutException timeoutException) {
            // re-throw to trigger `task.timeout.ms`
            throw timeoutException;
        } catch (final KafkaException error) {
            throw new StreamsException(
                formatException("Error encountered trying to commit a transaction"),
                error
            );
        }
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    void abortTransaction() {
        if (!eosEnabled()) {
            throw new IllegalStateException(formatException("Exactly-once is not enabled"));
        }
        if (transactionInFlight) {
            try {
                producer.abortTransaction();
            } catch (final TimeoutException logAndSwallow) {
                // no need to re-throw because we abort a TX only if we close a task dirty,
                // and thus `task.timeout.ms` does not apply
                log.warn(
                    "Aborting transaction failed due to timeout." +
                        " Will rely on broker to eventually abort the transaction after the transaction timeout passed.",
                    logAndSwallow
                );
            } catch (final ProducerFencedException | InvalidProducerEpochException | InvalidPidMappingException error) {
                // The producer is aborting the txn when there's still an ongoing one,
                // which means that we did not commit the task while closing it, which
                // means that it is a dirty close. Therefore it is possible that the dirty
                // close is due to an fenced exception already thrown previously, and hence
                // when calling abortTxn here the same exception would be thrown again.
                // Even if the dirty close was not due to an observed fencing exception but
                // something else (e.g. task corrupted) we can still ignore the exception here
                // since transaction already got aborted by brokers/transactional-coordinator if this happens
                log.debug("Encountered {} while aborting the transaction; this is expected and hence swallowed", error.getMessage());
            } catch (final KafkaException error) {
                throw new StreamsException(
                    formatException("Error encounter trying to abort a transaction"),
                    error
                );
            }
            transactionInFlight = false;
        }
    }

    /**
     * Cf {@link KafkaProducer#partitionsFor(String)}
     */
    List<PartitionInfo> partitionsFor(final String topic) {
        return producer.partitionsFor(topic);
    }

    Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    void flush() {
        producer.flush();
    }

    void close() {
        producer.close();
        transactionInFlight = false;
        transactionInitialized = false;
    }

    // for testing only
    Producer<byte[], byte[]> kafkaProducer() {
        return producer;
    }
}
