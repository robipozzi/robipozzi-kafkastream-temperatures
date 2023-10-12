package com.rpozzi.kafkastreams.service;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TemperatureAggregateSerde implements Serde<TemperatureAggregate> {

    @Override
    public Serializer<TemperatureAggregate> serializer() {
        return new TemperatureAggregateSerializer();
    }

    @Override
    public Deserializer<TemperatureAggregate> deserializer() {
        return new TemperatureAggregateDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No additional configuration is needed for this Serde.
    }

    @Override
    public void close() {
        // No resources to close.
    }
}

class TemperatureAggregateSerializer implements Serializer<TemperatureAggregate> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed.
    }

    @Override
    public byte[] serialize(String topic, TemperatureAggregate data) {
        if (data == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.allocate(12); // Assuming a double and an int
        buffer.putInt(data.getSum());
        buffer.putInt(data.getCount());
        return buffer.array();
    }

    @Override
    public void close() {
        // No resources to close.
    }
}

class TemperatureAggregateDeserializer implements Deserializer<TemperatureAggregate> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed.
    }

    @Override
    public TemperatureAggregate deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int sum = buffer.getInt();
        int count = buffer.getInt();
        return new TemperatureAggregate(sum, count);
    }

    @Override
    public void close() {
        // No resources to close.
    }
}