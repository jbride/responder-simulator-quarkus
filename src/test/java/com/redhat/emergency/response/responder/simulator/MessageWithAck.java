package com.redhat.emergency.response.responder.simulator;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Message;

public class MessageWithAck<T> implements Message<T> {

    private T payload;

    private boolean acked = false;

    private MessageWithAck() {}

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

    public boolean acked() {
        return acked;
    }
}
