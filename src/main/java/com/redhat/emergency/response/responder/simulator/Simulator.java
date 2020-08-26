package com.redhat.emergency.response.responder.simulator;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.redhat.emergency.response.responder.simulator.model.Coordinates;
import com.redhat.emergency.response.responder.simulator.model.MissionStep;
import com.redhat.emergency.response.responder.simulator.model.ResponderLocation;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.multi.processors.UnicastProcessor;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class Simulator {

    private final static Logger log = LoggerFactory.getLogger(Simulator.class);

    @Inject
    ResponderService responderService;

    @Inject
    ResponderLocationRepository repository;

    @ConfigProperty(name = "simulator.delay")
    long delay;

    @ConfigProperty(name = "simulator.distance.base")
    double baseDistance;

    @ConfigProperty(name = "simulator.distance.variation")
    double distanceVariation;

    private final UnicastProcessor<Pair<String, ResponderLocation>> processor = UnicastProcessor.create();

    @ConsumeEvent("simulator-mission-created")
    public void processMissionCreated(Message<JsonObject> message) {
        toResponderLocation(message.body()).onItem().transform(r -> repository.put(r))
                .invoke(this::waitForLocationUpdate)
                .subscribe().with(key -> message.replyAndForget(new JsonObject()), throwable -> {
                    log.error("Error while processing message with missionId " + message.body().getString("id"), throwable);
                    message.replyAndForget(new JsonObject());
                });
    }

    @ConsumeEvent("simulator-reponderlocation-status")
    public void processResponderLocationStatus(Message<JsonObject> message) {
        message.replyAndForget(new JsonObject());
        String missionId = message.body().getString("missionId");
        String status = message.body().getString("status");
        if (ResponderLocation.Status.PICKEDUP.name().equalsIgnoreCase(status)) {
            log.info("Processing responderlocation status update for ResponderLocation " + missionId);
            ResponderLocation responderLocation = repository.get(missionId);
            if (responderLocation == null) {
                log.warn("ResponderLocation " + missionId + " not found in repository.");
                return;
            }
            responderLocation.continueMoving(ResponderLocation.Status.PICKEDUP);
            repository.put(responderLocation);
            processor.onNext(new ImmutablePair<>(responderLocation.getResponderId(), responderLocation));
            waitForLocationUpdate(missionId);
        }

    }

    @Outgoing("responder-location-update")
    public Multi<org.eclipse.microprofile.reactive.messaging.Message<String>> responderLocationUpdateEvent() {

        return processor.onItem().transform(p -> {
            ResponderLocation rl = p.getRight();
            String json = new JsonObject().put("responderId", rl.getResponderId())
                    .put("missionId", rl.getMissionId())
                    .put("incidentId", rl.getIncidentId())
                    .put("status", rl.getStatus().name())
                    .put("lat", rl.getCurrentPosition().getLatD())
                    .put("lon", rl.getCurrentPosition().getLonD())
                    .put("human", rl.isPerson())
                    .put("continue", rl.getStatus().equals(ResponderLocation.Status.MOVING)
                            || rl.getStatus().equals(ResponderLocation.Status.PICKEDUP))
                    .encode();
            log.debug("Sending message to responder-location-update channel. Key: " + p.getLeft() + " - Message: " + json);
            return KafkaRecord.of(p.getLeft(), json);
        });

    }

    private void waitForLocationUpdate(String key) {
        Uni.createFrom().item(key).onItem().delayIt().by(Duration.ofMillis(delay))
                .onItem().invoke(this::processLocationUpdate)
                .subscribe().with(unused -> {});
    }

    private void processLocationUpdate(String key) {
        log.debug("Processing location update for ResponderLocation " + key);
        ResponderLocation responderLocation = repository.get(key);
        if (responderLocation == null) {
            log.warn("ResponderLocation " + key + " not found in repository.");
            return;
        }
        responderLocation.calculateNextLocation();
        responderLocation.moveToNextLocation();
        if (responderLocation.getStatus().equals(ResponderLocation.Status.MOVING) || responderLocation.getStatus().equals(ResponderLocation.Status.PICKEDUP)) {
            waitForLocationUpdate(key);
        }
        if (responderLocation.getStatus().equals(ResponderLocation.Status.DROPPED)) {
            repository.remove(key);
        } else {
            repository.put(responderLocation);
        }
        processor.onNext(new ImmutablePair<>(responderLocation.getResponderId(), responderLocation));
    }


    private Uni<ResponderLocation> toResponderLocation(JsonObject json) {
        return responderService.isPerson(json.getString("responderId"))
                .onItem().transform(person -> {
                    List<MissionStep> steps = json.getJsonArray("steps").stream().map(o -> (JsonObject) o)
                            .map(j -> new MissionStep(new Coordinates(BigDecimal.valueOf(j.getDouble("lat")), BigDecimal.valueOf(j.getDouble("lon"))),
                                    j.getBoolean("wayPoint"), j.getBoolean("destination"))).collect(Collectors.toList());
                    Coordinates currentPosition = new Coordinates(BigDecimal.valueOf(json.getDouble("responderStartLat")),
                            BigDecimal.valueOf((json.getDouble("responderStartLong"))));
                    return new ResponderLocation(json.getString("id"), json.getString("responderId"), json.getString("incidentId"), steps, currentPosition, person, distanceUnit());
                });
    }

    private double distanceUnit() {
        double max = baseDistance * (1 + distanceVariation);
        double min = baseDistance * (1 - distanceVariation);
        return ThreadLocalRandom.current().nextDouble(max - min) + min;
    }
}
