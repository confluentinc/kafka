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
package org.apache.kafka.tools;

import joptsimple.OptionException;
import joptsimple.OptionSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static joptsimple.util.RegexMatcher.regex;
import static org.apache.kafka.tools.ConsumerPerformance.printConsumerProgress;

public class ShareConsumerPerformance {
    private static final Logger LOG = LoggerFactory.getLogger(ShareConsumerPerformance.class);

    public static void main(String[] args) {
        try {
            LOG.info("Starting share consumer...");
            ShareConsumerPerfOptions options = new ShareConsumerPerfOptions(args);
            AtomicLong totalMessagesRead = new AtomicLong(0);
            AtomicLong totalBytesRead = new AtomicLong(0);
            AtomicLong joinTimeMs = new AtomicLong(0);
            AtomicLong joinTimeMsInSingleRound = new AtomicLong(0);

            if (!options.hideHeader())
                printHeader(options.showDetailedStats());

            KafkaShareConsumer<byte[], byte[]> shareConsumer = new KafkaShareConsumer<>(options.props());
            long bytesRead = 0L;
            long messagesRead = 0L;
            long lastBytesRead = 0L;
            long lastMessagesRead = 0L;
            long currentTimeMs = System.currentTimeMillis();
            long startMs = currentTimeMs;
            consume(shareConsumer, options, totalMessagesRead, totalBytesRead, bytesRead, messagesRead, lastBytesRead,
                    lastMessagesRead, joinTimeMsInSingleRound);
            long endMs = System.currentTimeMillis();

            Map<MetricName, ? extends Metric> metrics = null;
            if (options.printMetrics())
                metrics = shareConsumer.metrics();
            shareConsumer.commitAsync();
            shareConsumer.close();

            // print final stats
            double elapsedSec = (endMs - startMs) / 1_000.0;
            long fetchTimeInMs = (endMs - startMs) - joinTimeMs.get();
            if (!options.showDetailedStats()) {
                double totalMbRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
                System.out.printf("%s, %s, %.4f, %.4f, %.4f, %d, %d%n",
                    options.dateFormat().format(startMs),
                    options.dateFormat().format(endMs),
                    totalMbRead,
                    totalMbRead / elapsedSec,
                    totalMessagesRead.get() / elapsedSec,
                    totalMessagesRead.get(),
                    fetchTimeInMs
                );
            }

            if (metrics != null)
                ToolsUtils.printMetrics(metrics);
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            Exit.exit(1);
        }
    }

    protected static void printHeader(boolean showDetailedStats) {
        String newFieldsInHeader = ", fetch.time.ms";
        if (!showDetailedStats)
            System.out.printf("start.time, end.time, data.consumed.in.MB, MB.sec, nMsg.sec, data.consumed.in.nMsg%s%n", newFieldsInHeader);
        else
            System.out.printf("time, threadId, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec%s%n", newFieldsInHeader);
    }

    private static void consume(KafkaShareConsumer<byte[], byte[]> shareConsumer,
                                ShareConsumerPerfOptions options,
                                AtomicLong totalMessagesRead,
                                AtomicLong totalBytesRead,
                                long bytesRead,
                                long messagesRead,
                                long lastBytesRead,
                                long lastMessagesRead,
                                AtomicLong joinTimeMsInSingleRound) {
        long numMessages = options.numMessages();
        long recordFetchTimeoutMs = options.recordFetchTimeoutMs();
        long reportingIntervalMs = options.reportingIntervalMs();
        boolean showDetailedStats = options.showDetailedStats();
        SimpleDateFormat dateFormat = options.dateFormat();
        shareConsumer.subscribe(options.topic());

        // now start the benchmark
        long currentTimeMs = System.currentTimeMillis();
        long lastReportTimeMs = currentTimeMs;
        long lastConsumedTimeMs = currentTimeMs;

        while (messagesRead < numMessages && currentTimeMs - lastConsumedTimeMs <= recordFetchTimeoutMs) {
            ConsumerRecords<byte[], byte[]> records = shareConsumer.poll(Duration.ofMillis(100));
            currentTimeMs = System.currentTimeMillis();
            if (!records.isEmpty())
                lastConsumedTimeMs = currentTimeMs;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                messagesRead += 1;
                if (record.key() != null)
                    bytesRead += record.key().length;
                if (record.value() != null)
                    bytesRead += record.value().length;
                if (currentTimeMs - lastReportTimeMs >= reportingIntervalMs) {
                    if (showDetailedStats)
                        printConsumerProgress(0, bytesRead, lastBytesRead, messagesRead, lastMessagesRead,
                            lastReportTimeMs, currentTimeMs, dateFormat, joinTimeMsInSingleRound.get());
                    joinTimeMsInSingleRound = new AtomicLong(0);
                    lastReportTimeMs = currentTimeMs;
                    lastMessagesRead = messagesRead;
                    lastBytesRead = bytesRead;
                }
            }
        }

        if (messagesRead < numMessages)
            System.out.printf("WARNING: Exiting before consuming the expected number of messages: timeout (%d ms) exceeded. " +
                "You can use the --timeout option to increase the timeout.%n", recordFetchTimeoutMs);
        totalMessagesRead.set(messagesRead);
        totalBytesRead.set(bytesRead);
    }

    protected static class ShareConsumerPerfOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> groupIdOpt;
        private final OptionSpec<Integer> fetchSizeOpt;
        private final OptionSpec<Integer> socketBufferSizeOpt;
        private final OptionSpec<String> consumerConfigOpt;
        private final OptionSpec<Void> printMetricsOpt;
        private final OptionSpec<Void> showDetailedStatsOpt;
        private final OptionSpec<Long> recordFetchTimeoutOpt;
        private final OptionSpec<Long> numMessagesOpt;
        private final OptionSpec<Long> reportingIntervalOpt;
        private final OptionSpec<String> dateFormatOpt;
        private final OptionSpec<Void> hideHeaderOpt;

        public ShareConsumerPerfOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
            groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("perf-share-consumer")
                .ofType(String.class);
            fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
            socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(2 * 1024 * 1024);
            consumerConfigOpt = parser.accepts("consumer.config", "Share consumer config properties file.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            printMetricsOpt = parser.accepts("print-metrics", "Print out the metrics.");
            showDetailedStatsOpt = parser.accepts("show-detailed-stats", "If set, stats are reported for each reporting " +
                "interval as configured by reporting-interval");
            recordFetchTimeoutOpt = parser.accepts("timeout", "The maximum allowed time in milliseconds between returned records.")
                .withOptionalArg()
                .describedAs("milliseconds")
                .ofType(Long.class)
                .defaultsTo(10_000L);
            numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send or consume")
                .withRequiredArg()
                .describedAs("count")
                .ofType(Long.class);
            reportingIntervalOpt = parser.accepts("reporting-interval", "Interval in milliseconds at which to print progress info.")
                .withRequiredArg()
                .withValuesConvertedBy(regex("^\\d+$"))
                .describedAs("interval_ms")
                .ofType(Long.class)
                .defaultsTo(5_000L);
            dateFormatOpt = parser.accepts("date-format", "The date format to use for formatting the time field. " +
                    "See java.text.SimpleDateFormat for options.")
                .withRequiredArg()
                .describedAs("date format")
                .ofType(String.class)
                .defaultsTo("yyyy-MM-dd HH:mm:ss:SSS");
            hideHeaderOpt = parser.accepts("hide-header", "If set, skips printing the header for the stats");
            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
                return;
            }
            if (options != null) {
                CommandLineUtils.maybePrintHelpOrVersion(this, "This tool is used to verify the share consumer performance.");
                CommandLineUtils.checkRequiredArgs(parser, options, topicOpt, numMessagesOpt);
            }
        }

        public boolean printMetrics() {
            return options.has(printMetricsOpt);
        }

        public String brokerHostsAndPorts() {
            return options.valueOf(bootstrapServerOpt);
        }

        public Properties props() throws IOException {
            Properties props = (options.has(consumerConfigOpt))
                ? Utils.loadProps(options.valueOf(consumerConfigOpt))
                : new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsAndPorts());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt));
            props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, options.valueOf(socketBufferSizeOpt).toString());
            props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, options.valueOf(fetchSizeOpt).toString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            props.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
            if (props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null)
                props.put(ConsumerConfig.CLIENT_ID_CONFIG, "perf-share-consumer-client");
            return props;
        }

        public Set<String> topic() {
            return Collections.singleton(options.valueOf(topicOpt));
        }

        public long numMessages() {
            return options.valueOf(numMessagesOpt);
        }

        public long reportingIntervalMs() {
            long value = options.valueOf(reportingIntervalOpt);
            if (value <= 0)
                throw new IllegalArgumentException("Reporting interval must be greater than 0.");
            return value;
        }

        public boolean showDetailedStats() {
            return options.has(showDetailedStatsOpt);
        }

        public SimpleDateFormat dateFormat() {
            return new SimpleDateFormat(options.valueOf(dateFormatOpt));
        }

        public boolean hideHeader() {
            return options.has(hideHeaderOpt);
        }

        public long recordFetchTimeoutMs() {
            return options.valueOf(recordFetchTimeoutOpt);
        }
    }
}
