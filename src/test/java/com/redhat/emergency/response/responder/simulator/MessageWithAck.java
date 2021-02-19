package com.redhat.emergency.response.responder.simulator;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import io.smallrye.reactive.messaging.ce.DefaultCloudEventMetadataBuilder;
import io.smallrye.reactive.messaging.ce.impl.DefaultIncomingCloudEventMetadata;
import io.smallrye.reactive.messaging.kafka.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Message;

public class MessageWithAck<T> implements Message<T> {

    private T payload;

    private boolean acked = false;

    private IncomingKafkaRecordMetadata<String, T> incomingKafkaRecordMetadata;

    private DefaultIncomingCloudEventMetadata<T> defaultIncomingCloudEventMetadata;

    private MessageWithAck() {}

    static <T> MessageWithAck<T> of(T payload, String topic, int partition, long offset, boolean cloudEvent, String dataContentType, String messageType) {
        MessageWithAck<T> m = new MessageWithAck<>();
        m.payload = payload;
        m.incomingKafkaRecordMetadata = new IncomingKafkaRecordMetadata<>(new io.vertx.mutiny.kafka.client.consumer.KafkaConsumerRecord<>(new KafkaConsumerRecord<String, T>(topic, partition, offset)));
        if (cloudEvent) {
            DefaultCloudEventMetadataBuilder<T> defaultCloudEventMetadataBuilder = new DefaultCloudEventMetadataBuilder<T>();
            defaultCloudEventMetadataBuilder.withId(UUID.randomUUID().toString()).withSource(URI.create("emergency-response/test"))
                    .withTimestamp(OffsetDateTime.now().toZonedDateTime()).withType(messageType);
            if (dataContentType != null) {
                defaultCloudEventMetadataBuilder.withDataContentType(dataContentType);
            }
            m.defaultIncomingCloudEventMetadata = new DefaultIncomingCloudEventMetadata<>(defaultCloudEventMetadataBuilder.build());
        }
        return m;
    }

    static <T> MessageWithAck<T> of(T payload) {
        MessageWithAck<T> message = new MessageWithAck<>();
        message.payload = payload;
        return message;
    }

    @Override
    public CompletionStage<Void> ack() {
        acked = true;
        return getAck().get();
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> Optional<M> getMetadata(Class<? extends M> clazz) {
        if (clazz.isInstance(incomingKafkaRecordMetadata)) {
            return (Optional<M>) Optional.of(incomingKafkaRecordMetadata);
        } else if (clazz.isInstance(defaultIncomingCloudEventMetadata)) {
            return (Optional<M>) Optional.of(defaultIncomingCloudEventMetadata);
        } else {
            return Optional.empty();
        }

    }

    public boolean acked() {
        return acked;
    }
}
