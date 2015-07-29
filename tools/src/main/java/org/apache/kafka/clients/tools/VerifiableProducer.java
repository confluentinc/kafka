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

package org.apache.kafka.clients.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Primarily intended for use with system testing, this producer prints metadata
 * in the form of JSON to stdout on each "send" request. For example, this helps
 * with end-to-end correctness tests by making externally visible which messages have been
 * acked and which have not.
 *
 * When used as a command-line tool, it produces increasing integers. It will produce a 
 * fixed number of messages unless the default max-messages -1 is used, in which case
 * it produces indefinitely.
 *  
 * If logging is left enabled, log output on stdout can be easily ignored by checking
 * whether a given line is valid JSON.
 */
public class VerifiableProducer {
    
    String topic;
    private Producer<String, String> producer;
    
    // Print more detailed information if true
    private final boolean verbose;
  
    // If maxMessages < 0, produce until the process is killed externally
    private final long maxMessages;

    // Throttle message throughput if this is set >= 0
    private final long throughput;

    // Timeout on producer.close() call
    private final long closeTimeoutMs;

    private Properties producerProps;
  
    // Number of messages for which acks were received
    private long numAcked = 0;
    
    // Number of send attempts
    private long numSent = 0;
  
    // Hook to trigger producing thread to stop sending messages
    private AtomicBoolean stopProducing = new AtomicBoolean(false);
  
    // Track when this.close() is called
    private long timeOfCloseMs;

    public VerifiableProducer(
            Properties producerProps, String topic, int throughput, 
            int maxMessages, long closeTimeoutMs, boolean verbose) {

        this.topic = topic;
        this.throughput = throughput;
        this.maxMessages = maxMessages;
        this.closeTimeoutMs = closeTimeoutMs;
        this.verbose = verbose;
        this.producerProps = producerProps;
        this.producer = new KafkaProducer<>(producerProps);
    }
  
    @Override
    public String toString() {
        return this.getClass() + 
               "(producerProps=" + producerProps + ", " +
               "topic=" + topic + ", " +
               "throughput=" + throughput + ", " +
               "maxMessages=" + maxMessages + ", " +
               "closeTimeoutMs=" + closeTimeoutMs + ", " +
               "verbose=" + verbose + ")";
    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("verifiable-producer")
                .defaultHelp(true)
                .description("This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each \"send\" request, making externally visible which messages have been acked and which have not.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce messages to this topic.");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help(
                    "Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");
        
        parser.addArgument("--max-messages")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("MAX-MESSAGES")
                .dest("maxMessages")
                .help(
                    "Produce this many messages. If -1, produce messages until the process is killed externally.");

        parser.addArgument("--throughput")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help(
                    "If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.");

        parser.addArgument("--request-required-acks")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .choices(0, 1, -1)
                .metavar("REQUEST-REQUIRED-ACKS")
                .dest("acks")
                .help(
                    "Acks required on each produced message. See Kafka docs on request.required.acks for details.");
        
        parser.addArgument("--config")
                .action(Arguments.append())
                .required(false)
                .setDefault(new ArrayList<String>())
                .type(String.class)
                .dest("userSpecifiedConfigs")
                .metavar("CONFIG")
                .help("Any custom producer properties of the form key=val.");

        parser.addArgument("--close-timeout-ms")
                .action(store())
                .required(false)
                .setDefault(Long.MAX_VALUE)
                .type(Long.class)
                .metavar("CLOSE-TIMEOUT-MS")
                .dest("closeTimeoutMs")
                .help(
                    "When SIGTERM is caught, wait at most this many milliseconds for unsent messages to flush before stopping the VerifiableProducer process.");

        parser.addArgument("--verbose")
                .action(storeTrue())
                .required(false)
                .type(Boolean.class)
                .metavar("VERBOSE")
                .dest("verbose")
                .help("");

        return parser;
    }
  
    /** Construct a VerifiableProducer object from command-line arguments. */
    public static VerifiableProducer createFromArgs(String[] args) {
        ArgumentParser parser = argParser();
        VerifiableProducer producer = null;
        
        try {
            Namespace res = parser.parseArgs(args);
            int maxMessages = res.getInt("maxMessages");
            String topic = res.getString("topic");
            int throughput = res.getInt("throughput");
            long closeTimeoutMs = res.getLong("closeTimeoutMs");
            boolean verbose = res.getBoolean("verbose");
            
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                              "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                              "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.ACKS_CONFIG, Integer.toString(res.getInt("acks")));
            // No producer retries
            producerProps.put(ProducerConfig.RETRIES_CONFIG, "0");
            
            // Properties specified using the --config key=val format override
            // other properties
            List<String> userSpecifiedConfigs = res.getList("userSpecifiedConfigs");
            System.out.println(userSpecifiedConfigs);
            for (String config: userSpecifiedConfigs) {
                int index = config.indexOf('=');
                if (index < 0 || index == config.length() - 1) {
                    throw new IllegalArgumentException(
                            "Invalid format for producer config: " + config);
                }
                
                String key = config.substring(0, index);
                String val = config.substring(index + 1, config.length());
                producerProps.put(key, val);
            }

            producer = new VerifiableProducer(
                    producerProps, topic, throughput, maxMessages, closeTimeoutMs, verbose);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
        
        return producer;
    }
  
    /** Produce a message with given key and value. */
    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        numSent++;
        try {
            producer.send(record, new PrintInfoCallback(key, value));
        } catch (Exception e) {
            synchronized (System.out) {
                reportError(e, key, value);
                
            }
        }
    }
  
    private void reportSuccess(RecordMetadata recordMetadata, String key, String value) {
        if (verbose) {
            System.out.println(
                    successString(recordMetadata, key, value, System.currentTimeMillis()));
        } else {
            String data = "{\"p\":" + recordMetadata.partition();
            // Only add non-empty key
            data += (key == null || key.length() == 0) ? "" : ",\"k\":\"" + key + "\"";
            data += ",\"v\":\"" + value + "\"}";
            
            System.out.println(data);
        }
    }
    
    /** Report send error if in verbose mode. */
    private void reportError(Exception e, String key, String value) {
        if (verbose) {
            System.out.println(errorString(e, key, value, System.currentTimeMillis()));            
        }
    }
  
    /** Close the producer to flush any remaining messages. */
    public void close() {
        // Trigger main thread to stop producing messages
        stopProducing.set(true);
        timeOfCloseMs = System.currentTimeMillis();
        producer.close(this.closeTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Helper method used in constructing JSON strings. 
     * Although using a JSON library is more elegant, it's also significantly slower than
     * directly creating the small strings we need for this tool.
     */
    private static StringBuilder keyValToString(String key, String val) {
        StringBuilder builder = new StringBuilder();
        builder.append("\"");
        builder.append(key);
        builder.append("\":");

        builder.append("\"");
        builder.append(val);
        builder.append("\"");
        return builder;
    }
    
    private static String toJsonString(Map<String, Object> map) {
        StringBuilder builder = new StringBuilder();
        
        builder.append("{");
        int i = 0;
        for (String key: map.keySet()) {
            builder.append(keyValToString(key, String.valueOf(map.get(key))));
            if (i < map.keySet().size() - 1) {
                builder.append(",");
            }
            i++;
        }
        builder.append("}");
        return builder.toString();
    }
  
    String successString(RecordMetadata recordMetadata, String key, String value, Long nowMs) {
        assert recordMetadata != null : "Expected non-null recordMetadata object.";

        Map<String, Object> map = new HashMap<>();
        map.put("class", this.getClass().toString());
        map.put("name", "producer_send_success");
        map.put("time_ms", nowMs);
        map.put("topic", this.topic);
        map.put("partition", String.valueOf(recordMetadata.partition()));
        map.put("offset", String.valueOf(recordMetadata.offset()));
        map.put("key", key);
        map.put("value", value);

        return toJsonString(map);
    }

    /**
     * Return JSON string encapsulating basic information about the exception, as well
     * as the key and value which triggered the exception.
     */
    String errorString(Exception e, String key, String value, Long nowMs) {
        assert e != null : "Expected non-null exception.";

        Map<String, Object> map = new HashMap<>();
        map.put("class", this.getClass().toString());
        map.put("name", "producer_send_error");
        map.put("time_ms", nowMs);
        map.put("exception", e.getClass());
        map.put("message", e.getMessage());
        map.put("topic", this.topic);
        map.put("key", String.valueOf(key));
        map.put("value", String.valueOf(value));

        return toJsonString(map);
    }
  
    /** Callback which prints errors to stdout when the producer fails to send. */
    private class PrintInfoCallback implements Callback {
        
        private String key;
        private String value;
    
        PrintInfoCallback(String key, String value) {
            this.key = key;
            this.value = value;
        }
    
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          
            synchronized (System.out) {
                if (e == null) {
                    VerifiableProducer.this.numAcked++;
                    reportSuccess(recordMetadata, key, value);
                } else {
                    reportError(e, key, value);
                }
            }
        }
    }
  
    public static void main(String[] args) throws IOException {
        
        final VerifiableProducer producer = createFromArgs(args);
        System.out.println(producer);
      
        final long startMs = System.currentTimeMillis();
        boolean infinite = producer.maxMessages < 0;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("CAUGHT SIGTERM!! FLUSHING BUFFERED MESSAGES...");

                // Flush any remaining messages
                producer.close();
                long stopMs = System.currentTimeMillis();

                // Print a summary
                double avgThroughput = 1000 * ((producer.numAcked) / (double) (stopMs - startMs));

                Map<String, Object> map = new HashMap<>();
                map.put("class", this.getClass().toString());
                map.put("name", "tool_data");
                map.put("sent", producer.numSent);
                map.put("acked", producer.numAcked);
                map.put("target_throughput", producer.throughput);
                map.put("avg_throughput", avgThroughput);

                System.out.println(toJsonString(map));
            }
        });

        ThroughputThrottler throttler = new ThroughputThrottler(producer.throughput, startMs);
        for (int i = 0; i < producer.maxMessages || infinite; i++) {
            if (producer.stopProducing.get()) {
                break;
            }
            long sendStartMs = System.currentTimeMillis();
            producer.send(null, String.format("%d", i));
            
            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
    }
        
}
