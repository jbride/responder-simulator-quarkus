package com.redhat.emergency.response.responder.simulator;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.ce.IncomingCloudEventMetadata;
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
                .onItem().transform(this::accept)
                .onItem().ifNotNull().transformToUni(this::toSimulator)
                .onItem().transform(v -> missionCommandMessage.ack());
    }

    private Uni<Void> toSimulator(JsonObject payload) {
        return eventBus.request("simulator-mission-created", payload).map(m -> null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private JsonObject accept(Message<String> message) {
        Optional<IncomingCloudEventMetadata> metadata = message.getMetadata(IncomingCloudEventMetadata.class);
        if (metadata.isEmpty()) {
            log.warn("Incoming message is not a CloudEvent");
            return null;
        }
        IncomingCloudEventMetadata<String> cloudEventMetadata = metadata.get();
        String dataContentType = cloudEventMetadata.getDataContentType().orElse("");
        if (!dataContentType.equalsIgnoreCase("application/json")) {
            log.warn("CloudEvent data content type is not specified or not 'application/json'. Message is ignored");
            return null;
        }
        String type = cloudEventMetadata.getType();
        if (!(Arrays.asList(ACCEPTED_MESSAGE_TYPES).contains(type))) {
            log.debug("CloudEvent with type '" + type + "' is ignored");
            return null;
        }
        try {
            return new JsonObject(message.getPayload());
        } catch (Exception e) {
            log.warn("Unexpected message which is not JSON.");
            log.warn("Message: " + message.getPayload());
        }
        return null;
    }

}
