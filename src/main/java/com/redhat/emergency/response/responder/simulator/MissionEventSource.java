package com.redhat.emergency.response.responder.simulator;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MissionEventSource {

    private static final Logger log = LoggerFactory.getLogger(MissionEventSource.class);

    static final String MISSION_STARTED_EVENT = "MissionStartedEvent";
    static final String[] ACCEPTED_MESSAGE_TYPES = {MISSION_STARTED_EVENT};

    @Inject
    EventBus eventBus;

    @Incoming("mission-event")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    public Uni<CompletionStage<Void>> process(Message<String> missionCommandMessage) {
        return Uni.createFrom().item(missionCommandMessage)
                .onItem().transform(m -> accept(m.getPayload()))
                .onItem().ifNotNull().transformToUni(this::toSimulator)
                .onItem().transform(v -> missionCommandMessage.ack());
    }

    private Uni<Void> toSimulator(JsonObject payload) {
        return eventBus.request("simulator-mission-created", payload.getJsonObject("body")).map(m -> null);
    }

    private JsonObject accept(String messageAsJson) {
        try {
            JsonObject json = new JsonObject(messageAsJson);
            String messageType = json.getString("messageType");
            if (Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(messageType) && json.getJsonObject("body") != null) {
                log.info("Processing message: " + json.toString());
                return json;
            }
            log.info("Message with type '" + messageType + "' is ignored");
        } catch (Exception e) {
            log.warn("Unexpected message which is not JSON or without 'messageType' field.");
            log.warn("Message: " + messageAsJson);
        }
        return null;
    }

}
