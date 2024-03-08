package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;

import java.util.HashMap;
import java.util.Map;

public class Acknowledgements {
    private final Map<Long, AcknowledgeType> acknowledgements;

    public static Acknowledgements empty() { return new Acknowledgements(new HashMap<>()); }

    private Acknowledgements(Map<Long, AcknowledgeType> acknowledgements) {
        this.acknowledgements = acknowledgements;
    }

    public void add(long offset,
                    AcknowledgeType type) {
        this.acknowledgements.put(offset, type);
    }
}
