package com.redhat.emergency.response.responder.simulator;

import java.util.Collections;
import java.util.List;

import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;

public class KafkaConsumerRecord<K,V> implements io.vertx.kafka.client.consumer.KafkaConsumerRecord<K,V> {

    private final String topic;

    private final int partition;

    private final long offset;

    public KafkaConsumerRecord(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return 0;
    }

    @Override
    public TimestampType timestampType() {
        return null;
    }

    @Override
    public long checksum() {
        return 0;
    }

    @Override
    public K key() {
        return null;
    }

    @Override
    public V value() {
        return null;
    }

    @Override
    public List<KafkaHeader> headers() {
        return Collections.emptyList();
    }

    @Override
    public ConsumerRecord<K, V> record() {
        return null;
    }
}
