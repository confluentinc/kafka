package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.List;

public class ShareInFlightBatch<K, V> {
    final TopicIdPartition partition;
    private final Acknowledgements acknowledgements;
    List<ConsumerRecord<K, V>> inFlightRecords;

    public ShareInFlightBatch(TopicIdPartition partition) {
        this.partition = partition;
        acknowledgements = Acknowledgements.empty();
        inFlightRecords = new ArrayList<>();
    }

    public void addAcknowledgement(long offset, AcknowledgeType acknowledgeType) {
        acknowledgements.add(offset, acknowledgeType);
    }

    public void addRecord(ConsumerRecord<K, V> record) {
        inFlightRecords.add(record);
    }

    public boolean isEmpty() {
        return inFlightRecords.isEmpty();
    }
}
