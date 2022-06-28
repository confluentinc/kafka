package org.apache.kafka.clients;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeserializerTest {

    private byte[] serializeMetric;
    io.opentelemetry.proto.metrics.v1.Metric metric;

    @BeforeEach
    void setUp() {
        NumberDataPoint.Builder point = NumberDataPoint.newBuilder().setAsInt(15);
        Gauge gauge = Gauge.newBuilder().addDataPoints(point).build();

        metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("Gauge-Test-Metric")
                .setDescription("description")
                .build();


        Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("TestKey")
                        .setValue(AnyValue.newBuilder().setStringValue("TestValue").build())
                ).build();


        ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addInstrumentationLibraryMetrics(
                        InstrumentationLibraryMetrics.newBuilder().addMetrics(metric).build()
                ).build();


        MetricsData.Builder builder = MetricsData.newBuilder();
        builder.addResourceMetrics(rm);
        serializeMetric = builder.build().toByteArray();
    }

    @Test
    public void testDeserializeMetrics() {
        List<io.opentelemetry.proto.metrics.v1.Metric> deserializedMetrics;
        deserializedMetrics = ClientTelemetryUtils.deserializeMetrics(serializeMetric);

        assertFalse(deserializedMetrics.isEmpty());
        assertTrue(deserializedMetrics.contains(metric));
        assertEquals(metric, deserializedMetrics.get(0));
    }

}
